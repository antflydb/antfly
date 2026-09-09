//go:build runtimeintegration && (linux || darwin)

package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
)

// This deliberately executes the reconciler's output, not a separately written
// inference command. Only filesystem mounts and the listen address are mapped
// to an isolated local sandbox. No model flags may be added by this harness.
func TestInferenceRuntimeContract(t *testing.T) {
	binary := os.Getenv("ANTFLY_RUNTIME_BIN")
	if !filepath.IsAbs(binary) {
		t.Fatal("runtimeintegration requires ANTFLY_RUNTIME_BIN pointing to a real released or newly built runtime")
	}
	root := t.TempDir()
	modelDir := filepath.Join(root, "models")
	const model = "BAAI/bge-small-en-v1.5"
	for _, scenario := range []string{"eager", "lazy", "missing-directory-negative-control"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			scheme := newInferenceUnitTestScheme(g)
			pool := &api.InferencePool{
				ObjectMeta: metav1.ObjectMeta{Name: "runtime-smoke", Namespace: "default", UID: "smoke"},
				Spec: api.InferencePoolSpec{
					Hardware: api.HardwareConfig{InferenceBackend: api.InferenceRuntimeBackendCPU},
					Models:   api.ModelConfig{LoadingStrategy: api.LoadingStrategyLazy, Preload: []api.ModelSpec{{Name: model, Tasks: []string{"embed"}, Capabilities: []string{"text"}}}},
				},
			}
			if scenario == "eager" {
				pool.Spec.Models.LoadingStrategy = api.LoadingStrategyEager
			}
			client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
			r := &InferencePoolReconciler{Client: client, Scheme: scheme}
			g.Expect(r.reconcileConfigMap(ctx, pool)).To(Succeed())
			g.Expect(r.reconcileStatefulSet(ctx, pool)).To(Succeed())
			cm := &corev1.ConfigMap{}
			g.Expect(client.Get(ctx, types.NamespacedName{Name: pool.Name + "-config", Namespace: pool.Namespace}, cm)).To(Succeed())
			sts := &appsv1.StatefulSet{}
			g.Expect(client.Get(ctx, types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace}, sts)).To(Succeed())

			work := t.TempDir()
			g.Expect(os.MkdirAll(modelDir, 0755)).To(Succeed())
			configPath := filepath.Join(work, "config.json")
			var config map[string]any
			g.Expect(json.Unmarshal([]byte(cm.Data["config.json"]), &config)).To(Succeed())
			g.Expect(config["models_dir"]).To(Equal("/models"))
			config["models_dir"] = modelDir
			if scenario == "missing-directory-negative-control" {
				// New runtimes also read this path from config. Remove both
				// sources so the control remains valid across runtime releases.
				delete(config, "models_dir")
			}
			configJSON, err := json.Marshal(config)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(os.WriteFile(configPath, configJSON, 0600)).To(Succeed())
			env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + work, "TMPDIR=" + work, "XDG_CACHE_HOME=" + work}
			for key, value := range cm.Data {
				if key != "config.json" {
					env = append(env, key+"="+value)
				}
			}
			// Keep host credentials and cached models out of the child process.
			mapArgs := func(args []string) []string {
				mapped := append([]string(nil), args...)
				for i, arg := range mapped {
					switch arg {
					case "/models":
						mapped[i] = modelDir
					case "/config/config.json":
						mapped[i] = configPath
					}
				}
				return mapped
			}
			if scenario == "eager" {
				for _, init := range sts.Spec.Template.Spec.InitContainers {
					cmd := exec.CommandContext(ctx, binary, mapArgs(init.Args)...)
					cmd.Env = env
					out, err := cmd.CombinedOutput()
					if err != nil {
						t.Fatalf("operator model pull failed: %v\n%s", err, out)
					}
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			g.Expect(err).NotTo(HaveOccurred())
			port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
			g.Expect(listener.Close()).To(Succeed())
			args := mapArgs(sts.Spec.Template.Spec.Containers[0].Args)
			for i := 0; i < len(args)-1; i++ {
				switch args[i] {
				case "--host":
					args[i+1] = "127.0.0.1"
				case "--port":
					args[i+1] = port
				}
			}
			if scenario == "missing-directory-negative-control" {
				for i := 0; i < len(args)-1; i++ {
					if args[i] == "--models-dir" {
						args = append(args[:i], args[i+2:]...)
						break
					}
				}
			}
			logPath := filepath.Join(work, "runtime.log")
			log, err := os.Create(logPath)
			g.Expect(err).NotTo(HaveOccurred())
			cmd := exec.CommandContext(ctx, binary, args...)
			// Kill the isolated process group, including any runtime worker, on
			// timeout or cleanup. Never leave an inference process behind in CI.
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
			cmd.Env, cmd.Stdout, cmd.Stderr = env, log, log
			g.Expect(cmd.Start()).To(Succeed())
			defer func() {
				cancel()
				_ = cmd.Wait()
				_ = log.Close()
				if t.Failed() {
					out, _ := os.ReadFile(logPath)
					t.Logf("runtime output:\n%s", out)
				}
			}()
			httpClient := &http.Client{Timeout: 20 * time.Second}
			url := "http://127.0.0.1:" + port
			status := func(path string) int {
				resp, err := httpClient.Get(url + path)
				if err != nil {
					return 0
				}
				defer resp.Body.Close()
				return resp.StatusCode
			}
			g.Eventually(func() int { return status("/healthz") }, 90*time.Second, 250*time.Millisecond).Should(Equal(200))
			if scenario == "missing-directory-negative-control" {
				g.Consistently(func() int { return status("/readyz") }, 3*time.Second, 250*time.Millisecond).Should(Equal(503))
				return
			}
			g.Eventually(func() int { return status("/readyz") }, 90*time.Second, 250*time.Millisecond).Should(Equal(200))
			if scenario == "eager" {
				out, err := os.ReadFile(logPath)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(out)).To(ContainSubstring("warmed inference embedder model=" + model))
			}
			body := bytes.NewBufferString(`{"model":"` + model + `","input":"operator runtime contract smoke test"}`)
			resp, err := httpClient.Post(url+"/ai/v1/embed", "application/json", body)
			g.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(resp.StatusCode).To(Equal(200), string(data))
			var result struct {
				Data []struct {
					Embedding []float64 `json:"embedding"`
				} `json:"data"`
			}
			g.Expect(json.Unmarshal(data, &result)).To(Succeed())
			g.Expect(result.Data).To(HaveLen(1))
			g.Expect(result.Data[0].Embedding).To(HaveLen(384))
			var norm float64
			for _, value := range result.Data[0].Embedding {
				g.Expect(math.IsNaN(value) || math.IsInf(value, 0)).To(BeFalse())
				norm += value * value
			}
			g.Expect(norm).To(BeNumerically(">", 0))
			g.Expect(strings.Join(env, "\n")).To(ContainSubstring("ANTFLY_INFERENCE_REQUIRED_BACKEND=native"))
		})
	}
}
