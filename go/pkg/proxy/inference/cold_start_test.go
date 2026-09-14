package proxy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func coldFallbackProxy() *Proxy {
	p := NewProxy(Config{ColdStartFallbackRoutes: []string{"inference/gpu"}, Logger: zap.NewNop()})
	p.Router().RouteManager().AddRoute(&Route{Name: "inference/gpu", Operations: map[OperationType]bool{"extract": true}, Destinations: []Destination{{Pool: "gpu", Weight: 100}}, Fallback: &Fallback{Action: "redirect", RedirectPool: "cpu"}})
	return p
}

func TestColdFallbackDoesNotWaitAndCoalescesActivation(t *testing.T) {
	p := coldFallbackProxy()
	p.RegisterEndpoint("http://cpu", "cpu", WorkloadTypeReadHeavy)
	started, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	p.SetPoolActivator(testPoolActivator{enabled: func(_, pool string) bool { return pool == "gpu" }, activate: func(ctx context.Context, _, _ string) (time.Duration, bool, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		defer close(done)
		select {
		case <-finish:
			return time.Minute, true, nil
		case <-ctx.Done():
			return 0, true, ctx.Err()
		}
	}})
	ctx, cancel := context.WithCancel(context.Background())
	r, err := p.ResolveRequest(ctx, ResolveRequest{Operation: "extract", Model: "gliner2"})
	if err != nil || r.Pool != "cpu" || !r.ColdStartFallback {
		t.Fatalf("resolution=%+v err=%v", r, err)
	}
	cancel() // finishing the first CPU request must not kill activation
	<-started
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			l, e := p.AcquireRequestResolution(context.Background(), ResolveRequest{Operation: "extract", Model: "gliner2"})
			if e != nil {
				t.Error(e)
				return
			}
			if l.Resolution.Pool != "cpu" {
				t.Error("not CPU")
			}
			l.Release()
		})
	}
	wg.Wait()
	select {
	case <-done:
		t.Fatal("request cancellation killed activation")
	default:
	}
	if calls.Load() != 1 {
		t.Fatalf("activation calls=%d", calls.Load())
	}
	close(finish)
	<-done
}

func TestColdFallbackActivationFailureAndCPUUnavailable(t *testing.T) {
	for _, healthy := range []bool{true, false} {
		t.Run(map[bool]string{true: "CPU healthy", false: "CPU unavailable"}[healthy], func(t *testing.T) {
			p := coldFallbackProxy()
			if healthy {
				p.RegisterEndpoint("http://cpu", "cpu", WorkloadTypeReadHeavy)
			}
			done := make(chan struct{})
			p.SetPoolActivator(testPoolActivator{enabled: func(_, pool string) bool { return pool == "gpu" }, activate: func(context.Context, string, string) (time.Duration, bool, error) {
				defer close(done)
				return 0, true, errors.New("API unavailable")
			}})
			r, e := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "extract", Model: "gliner2"})
			if healthy {
				if e != nil || r.Pool != "cpu" {
					t.Fatalf("resolution=%+v err=%v", r, e)
				}
			} else {
				var re *ResolutionError
				if !errors.As(e, &re) || re.StatusCode != 503 || re.RetryAfter != 5 {
					t.Fatalf("err=%v", e)
				}
			}
			<-done
		})
	}
}

func TestColdFallbackWarmGPUPreferred(t *testing.T) {
	p := coldFallbackProxy()
	p.RegisterEndpoint("http://cpu", "cpu", WorkloadTypeReadHeavy)
	p.RegisterEndpoint("http://gpu", "gpu", WorkloadTypeReadHeavy)
	var calls atomic.Int32
	p.SetPoolActivator(testPoolActivator{activate: func(context.Context, string, string) (time.Duration, bool, error) {
		calls.Add(1)
		return time.Minute, true, nil
	}})
	r, e := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "extract", Model: "gliner2"})
	if e != nil || r.Pool != "gpu" || r.ColdStartFallback || calls.Load() != 1 {
		t.Fatalf("resolution=%+v err=%v renewals=%d", r, e, calls.Load())
	}
}

func TestColdFallbackActivationBounded(t *testing.T) {
	p := coldFallbackProxy()
	p.RegisterEndpoint("http://cpu", "cpu", WorkloadTypeReadHeavy)
	done := make(chan error, 1)
	p.SetPoolActivator(testPoolActivator{activate: func(ctx context.Context, _, _ string) (time.Duration, bool, error) {
		<-ctx.Done()
		done <- ctx.Err()
		return 0, true, ctx.Err()
	}})
	if _, e := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "extract"}); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if !errors.Is(e, context.DeadlineExceeded) {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("activation did not expire")
	}
}

func TestColdFallbackLeaseSurvivesCPUResponseAndExpiresWithoutTraffic(t *testing.T) {
	p := coldFallbackProxy()
	p.RegisterEndpoint("http://cpu", "cpu", WorkloadTypeReadHeavy)
	client := kubernetesfake.NewSimpleClientset()
	watcher := &K8sWatcher{clientset: client, scalePools: map[string]scaleToZeroPool{"inference/gpu": {namespace: "inference", uid: "gpu-uid", idleTimeout: 15 * time.Minute, activationTimeout: 5 * time.Minute}}}
	p.SetPoolActivator(watcher)
	r, e := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "extract"})
	if e != nil || r.Pool != "cpu" {
		t.Fatalf("%+v %v", r, e)
	}
	deadline := time.Now().Add(time.Second)
	for {
		lease, e := client.CoordinationV1().Leases("inference").Get(context.Background(), "gpu", metav1.GetOptions{})
		if e == nil {
			if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 900 || lease.Spec.RenewTime == nil {
				t.Fatalf("invalid lease: %+v", lease.Spec)
			}
			// No timer/worker should extend the activity after the single request.
			p.activationMu.Lock()
			active := len(p.activations)
			p.activationMu.Unlock()
			if active == 0 {
				if len(client.Actions()) < 2 {
					t.Fatal("missing lease write")
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("activation Lease not completed")
		}
		time.Sleep(time.Millisecond)
	}
}
