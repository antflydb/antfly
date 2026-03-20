package sim

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"go.etcd.io/raft/v3/raftpb"
)

type RaftMessageClass string

const (
	RaftMessageClassAppend        RaftMessageClass = "append"
	RaftMessageClassAppendResp    RaftMessageClass = "append_resp"
	RaftMessageClassVote          RaftMessageClass = "vote"
	RaftMessageClassVoteResp      RaftMessageClass = "vote_resp"
	RaftMessageClassHeartbeat     RaftMessageClass = "heartbeat"
	RaftMessageClassHeartbeatResp RaftMessageClass = "heartbeat_resp"
	RaftMessageClassSnapshot      RaftMessageClass = "snapshot"
)

type transportLink struct {
	From types.ID
	To   types.ID
}

type transportFaultTarget struct {
	ShardID types.ID
	Link    transportLink
	Class   RaftMessageClass
	Latency time.Duration
}

func classifyRaftMessageClass(msgType raftpb.MessageType) RaftMessageClass {
	switch msgType {
	case raftpb.MsgApp:
		return RaftMessageClassAppend
	case raftpb.MsgAppResp:
		return RaftMessageClassAppendResp
	case raftpb.MsgVote, raftpb.MsgPreVote:
		return RaftMessageClassVote
	case raftpb.MsgVoteResp, raftpb.MsgPreVoteResp:
		return RaftMessageClassVoteResp
	case raftpb.MsgHeartbeat:
		return RaftMessageClassHeartbeat
	case raftpb.MsgHeartbeatResp:
		return RaftMessageClassHeartbeatResp
	case raftpb.MsgSnap:
		return RaftMessageClassSnapshot
	default:
		return ""
	}
}

func transportMessageMatch(target transportFaultTarget) MessageMatch {
	return func(env MessageEnvelope) bool {
		return env.ShardID == target.ShardID &&
			env.From == target.Link.From &&
			env.To == target.Link.To &&
			classifyRaftMessageClass(env.Message.Type) == target.Class
	}
}

func (h *Harness) DropNextTransportMessage(target transportFaultTarget) {
	h.network.DropNext(transportMessageMatch(target))
	h.recordEvent("transport_fault", "action=%s shard=%s from=%s to=%s class=%s",
		ActionDropNextMsg, target.ShardID, target.Link.From, target.Link.To, target.Class)
}

func (h *Harness) DuplicateNextTransportMessage(target transportFaultTarget, extraCopies int) {
	h.network.DuplicateNext(transportMessageMatch(target), extraCopies)
	h.recordEvent("transport_fault", "action=%s shard=%s from=%s to=%s class=%s extra=%d",
		ActionDuplicateNextMsg, target.ShardID, target.Link.From, target.Link.To, target.Class, extraCopies)
}

func (h *Harness) CutTransportLink(target transportFaultTarget) {
	h.network.CutLink(target.Link.From, target.Link.To)
	h.recordEvent("transport_fault", "action=%s shard=%s from=%s to=%s",
		ActionCutLink, target.ShardID, target.Link.From, target.Link.To)
}

func (h *Harness) HealTransportLink(target transportFaultTarget) {
	h.network.HealLink(target.Link.From, target.Link.To)
	h.recordEvent("transport_fault", "action=%s shard=%s from=%s to=%s",
		ActionHealLink, target.ShardID, target.Link.From, target.Link.To)
}

func (h *Harness) SetTransportLinkLatency(target transportFaultTarget) {
	h.network.SetLinkLatency(target.Link.From, target.Link.To, target.Latency)
	h.recordEvent("transport_fault", "action=%s shard=%s from=%s to=%s latency=%s",
		ActionSetLinkLatency, target.ShardID, target.Link.From, target.Link.To, target.Latency)
}

func (h *Harness) ResetTransportLinkLatency(target transportFaultTarget) {
	h.network.ResetLinkLatency(target.Link.From, target.Link.To)
	h.recordEvent("transport_fault", "action=%s shard=%s from=%s to=%s",
		ActionResetLinkLatency, target.ShardID, target.Link.From, target.Link.To)
}

func selectTransportFaultTarget(
	ctx context.Context,
	h *Harness,
	rng *rand.Rand,
	classes []RaftMessageClass,
	allow func(transportLink) bool,
) (transportFaultTarget, error) {
	snapshot, err := h.Snapshot(ctx)
	if err != nil {
		return transportFaultTarget{}, err
	}
	candidates := collectTransportTargets(snapshot, classes, allow)
	if len(candidates) == 0 {
		return transportFaultTarget{}, fmt.Errorf("no transport fault target available")
	}
	return candidates[rng.Intn(len(candidates))], nil
}

func selectTransportLatency(rng *rand.Rand) time.Duration {
	options := []time.Duration{
		5 * time.Millisecond,
		25 * time.Millisecond,
		100 * time.Millisecond,
	}
	return options[rng.Intn(len(options))]
}

func collectTransportTargets(
	snapshot *ClusterSnapshot,
	classes []RaftMessageClass,
	allow func(transportLink) bool,
) []transportFaultTarget {
	if snapshot == nil {
		return nil
	}
	healthy := make(map[types.ID]bool, len(snapshot.Stores))
	for storeID, status := range snapshot.Stores {
		healthy[storeID] = status != nil && status.IsReachable()
	}

	shardIDs := make([]types.ID, 0, len(snapshot.Shards))
	for shardID := range snapshot.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)

	candidates := make([]transportFaultTarget, 0)
	for _, shardID := range shardIDs {
		status := snapshot.Shards[shardID]
		if status == nil || status.RaftStatus == nil {
			continue
		}
		voters := make([]types.ID, 0, len(status.RaftStatus.Voters))
		for voter := range status.RaftStatus.Voters {
			voters = append(voters, types.ID(voter))
		}
		slices.Sort(voters)
		if len(voters) < 2 {
			continue
		}
		leader := types.ID(status.RaftStatus.Lead)

		for _, class := range classes {
			switch class {
			case RaftMessageClassAppend, RaftMessageClassHeartbeat, RaftMessageClassSnapshot:
				if leader == 0 || !healthy[leader] {
					continue
				}
				for _, peer := range voters {
					if peer == leader || !healthy[peer] {
						continue
					}
					link := transportLink{From: leader, To: peer}
					if allow != nil && !allow(link) {
						continue
					}
					candidates = append(candidates, transportFaultTarget{
						ShardID: shardID,
						Link:    link,
						Class:   class,
					})
				}
			case RaftMessageClassAppendResp, RaftMessageClassHeartbeatResp:
				if leader == 0 || !healthy[leader] {
					continue
				}
				for _, peer := range voters {
					if peer == leader || !healthy[peer] {
						continue
					}
					link := transportLink{From: peer, To: leader}
					if allow != nil && !allow(link) {
						continue
					}
					candidates = append(candidates, transportFaultTarget{
						ShardID: shardID,
						Link:    link,
						Class:   class,
					})
				}
			case RaftMessageClassVote, RaftMessageClassVoteResp:
				for _, from := range voters {
					if !healthy[from] {
						continue
					}
					for _, to := range voters {
						if from == to || !healthy[to] {
							continue
						}
						link := transportLink{From: from, To: to}
						if allow != nil && !allow(link) {
							continue
						}
						candidates = append(candidates, transportFaultTarget{
							ShardID: shardID,
							Link:    link,
							Class:   class,
						})
					}
				}
			}
		}
	}

	return candidates
}
