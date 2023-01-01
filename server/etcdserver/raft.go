// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdserver

import (
	"expvar"
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.uber.org/zap"
)

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

var (
	// protects raftStatus
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
	expvar.Publish("raft.status", expvar.Func(func() interface{} {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()
		if raftStatus == nil {
			return nil
		}
		return raftStatus()
	}))
}

// toApply contains entries, snapshot to be applied. Once
// an toApply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type toApply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	// notifyc synchronizes etcd server applies with the raft node
	notifyc chan struct{}
}

type raftNode struct {
	lg *zap.Logger

	tickMu *sync.Mutex
	raftNodeConfig

	// a chan to send/receive snapshot
	// etcd-raft模块通过返回Ready实例与上层模块进行交互，其中Ready.Message字段记录了待发送的消息,
	// 其中可能会包含MsgSnap类型的消息,该消息类型中封装了需要发送到其他节点的快照数据。
	// 当raftNode收到MsgSnap消息之后,会将其写入msgSnapC通道中，并等待上层模块进行发送。
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	// 在etcd-raft模块返回的Ready实例中,除了封装了待持久化的Entry记录和待持久化的快照数据，还封装了待应用的Entry记录.
	// raftNode会将待应用的记录和快照数据封装成apply实例，之后写入applyc通道等待上层模块处理.
	applyc chan toApply

	// a chan to send out readState
	// Readyc.ReadStates中封装了只读请求相关的ReadState实例,
	// 其中的最后一项将会被写入readStateC通道中等待上层模块处理。
	readStateC chan raft.ReadState

	// utility
	// 该定时器就是逻辑时钟，每触发一次就会推进一次底层的选举计时器和心跳计时器。
	ticker *time.Ticker
	// contention detectors for raft heartbeat message
	// 检测发往同一节点的两次心跳消息是否超时, 如果超时, 则会打印相关警告消息。
	td *contention.TimeoutDetector

	stopped chan struct{}
	done    chan struct{}
}

type raftNodeConfig struct {
	lg *zap.Logger

	// to check if msg receiver is removed from cluster
	// 该函数用来检测指定节点是否已经被移出当前集群
	isIDRemoved func(id uint64) bool
	raft.Node
	// raftLog.storage字段指向的MemoryStorage为同一实例, 主要用来保存持久化的Entry记录和快照数据
	raftStorage *raft.MemoryStorage
	// 注意该字段的类型,在etcd-raft模块中有一个与之同名的接口(raft.Storage接口),
	// MemoryStorage就是raft.Storage接口的实现之一。
	storage serverstorage.Storage
	// 逻辑时钟的刻度
	heartbeat time.Duration // for logging
	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	// 通过网络将消息发送到集群中其他节点
	transport rafthttp.Transporter
}

// newRaftNode 创建raftNode实例, 创建各种通道
func newRaftNode(cfg raftNodeConfig) *raftNode {
	var lg raft.Logger
	if cfg.lg != nil {
		lg = NewRaftLoggerZap(cfg.lg)
	} else {
		lcfg := logutil.DefaultZapLoggerConfig
		var err error
		lg, err = NewRaftLogger(&lcfg)
		if err != nil {
			log.Fatalf("cannot create raft logger %v", err)
		}
	}
	raft.SetLogger(lg)
	r := &raftNode{
		lg:             cfg.lg,
		tickMu:         new(sync.Mutex),
		raftNodeConfig: cfg,
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.heartbeat),
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan toApply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if r.heartbeat == 0 { // 根据raftNodeConfig.hearbeat字段创建逻辑时钟,其时间刻度是hearbeat
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat) // 每隔heartbeat触发一次
	}
	return r
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
// start 启动一个独立的后台goroutine, 在后台goroutine中完成了绝大部分与底层etcd-raft模块交互的功能
func (r *raftNode) start(rh *raftReadyHandler) {
	internalTimeout := time.Second
	// 启动后台的goroutine提供服务
	go func() {
		defer r.onStop()
		islead := false // 刚启动时会将当前节点标识为follower

		for {
			select {
			case <-r.ticker.C: // 计时器到期被触发,调用Tick()方法推进选举计时器和心跳计时器
				r.tick()
			case rd := <-r.Ready():
				if rd.SoftState != nil { // 在SoftState中主要封装了当前集群的Leader信息和当前节点角色
					// 检测集群的Leader节点是否发生变化, 并记录相关监控信息
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					if newLeader { // 当前集群的Leader节点发生变更,记录下leader节点变化次数
						leaderChanges.Inc()
					}

					if rd.SoftState.Lead == raft.None { // 选举阶段，还未产生新的Leader
						hasLeader.Set(0)
					} else { // 集群Leader节点已选举完成，并且集群选举出了新的Leader节点
						hasLeader.Set(1)
					}
					// 更新raftNode.lead字段，将其更新未新的Leader节点ID,
					// 注意，这里读取和更新raftNode。lead字段都时原子操作
					rh.updateLead(rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader // 记录当前节点是否为Leader节点
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					// 调用raftReadyHandler中的updateLeadership()回调方法,
					// 其中会根据Leader节点是否发生变化完成一些操作
					rh.updateLeadership(newLeader)
					r.td.Reset() // 重置全部探测器中的全部记录
				}

				if len(rd.ReadStates) != 0 {
					select {
					// 将Ready.ReadStates中的最后一项写入readStateC通道中
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						// 如果上层应用一致没有读取写入readStateC通道中的ReadState实例,会导致本次写入阻塞,
						// 这里会等待1秒, 如果依然无法写入,则放弃写入并输出警告日志。
						r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
					case <-r.stopped:
						return
					}
				}
				// 4. 对Ready实例中待应用的Entry记录,以及快照数据的处理流程
				notifyc := make(chan struct{}, 1) // 创建notifyc通道
				// 将Ready实例中的待应用Entry记录以及快照数据封装成apply实例，其中封装了notifyc通道，
				// 该通道用来协调当前goroutine和EtcdServer启动的后台 goroutine 的执行
				ap := toApply{
					entries:  rd.CommittedEntries, // 已提交,待应用的Entry记录
					snapshot: rd.Snapshot,         // 待持久化的快照数据
					notifyc:  notifyc,
				}
				// 更新EtcdServer中记录的已提交位置(EtcdServer.committedIndex字段)
				updateCommittedIndex(&ap, rh)

				select {
				case r.applyc <- ap: // 将apply实例写入applyc通道中,等待上层应用读取进行处理
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and them
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					// 如果当前节点为Leader状态,则调用raftNode.processMessages()方法对待发送的消息进行过滤,
					// 然后调用raftNode.transport.Send()方法完成消息的发送
					r.transport.Send(r.processMessages(rd.Messages))
				}

				// Must save the snapshot file and WAL snapshot entry before saving any other entries or hardstate to
				// ensure that recovery after a snapshot restore is possible.
				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					// 通过raftNode.storage将Ready实例中携带的快照数据保存到磁盘中,错误处理
					if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
					}
					// gofail: var raftAfterSaveSnap struct{}
				}

				// gofail: var raftBeforeSave struct{}
				// 通过raftNode.storage将Ready实例中携带的HardState信息和待持久化的Entry记录写入WAL日志文件中
				if err := r.storage.Save(rd.HardState, rd.Entries); err != nil {
					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// Force WAL to fsync its hard state before Release() releases
					// old data from the WAL. Otherwise could get an error like:
					// panic: tocommit(107) is out of range [lastIndex(84)]. Was the raft log corrupted, truncated, or lost?
					// See https://github.com/etcd-io/etcd/issues/10219 for more details.
					if err := r.storage.Sync(); err != nil {
						r.lg.Fatal("failed to sync Raft snapshot", zap.Error(err))
					}

					// etcdserver now claim the snapshot has been persisted onto the disk
					// etcdserver通知后台goroutine该apply实例中的快照数据已经被持久化到磁盘,后台goroutine可以开始应用该快照数据了
					notifyc <- struct{}{}

					// gofail: var raftBeforeApplySnap struct{}
					r.raftStorage.ApplySnapshot(rd.Snapshot) // 将快照数据保存到MemoryStorage中
					r.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					// gofail: var raftAfterApplySnap struct{}
					// 根据WAL日志文件名称及快照数据的元数据,释放快照之前的WAL日志文件句柄
					if err := r.storage.Release(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to release Raft wal", zap.Error(err))
					}
					// gofail: var raftAfterWALRelease struct{}
				}
				// 将待持久化的Entry记录写入MemoryStorage中
				r.raftStorage.Append(rd.Entries)

				if !islead {
					// finish processing incoming messages before we signal raftdone chan
					// 调用raftNode.processMessages()方法对待发送的消息进行过滤
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					// 处理Ready实例的过程基本结束, 通知EtcdServer启动的后台goroutine,检测是否生成快照
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before toApply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					waitApply := false
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					if waitApply {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					// 发送消息
					r.transport.Send(msgs)
				} else {
					// leader already processed 'MsgSnap' and signaled
					notifyc <- struct{}{}
				}
				// 最后调用raft.node.Advance()方法,通知etcd-raft模块此次Ready实例已经处理完成,
				// etcd-raft模块更新相应信息(例如,已应用Entry的最大索引值)之后,可以继续返回Ready实例
				r.Advance()
			case <-r.stopped:
				return // 当前节点已停止
			}
		}
	}()
}

func updateCommittedIndex(ap *toApply, rh *raftReadyHandler) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}

// processMessages 对消息进行过滤,去除掉目标节点已被移出集群的消息,
// 然后分别过滤MsgAppResp消息,MsgSnap消息和MsgHeartBeat消息
func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.isIDRemoved(ms[i].To) { // 去除掉目标节点已被移出集群的消息
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				r.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.String("to", fmt.Sprintf("%x", ms[i].To)),
					zap.Duration("heartbeat-interval", r.heartbeat),
					zap.Duration("expected-duration", 2*r.heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

func (r *raftNode) apply() chan toApply {
	return r.applyc
}

func (r *raftNode) stop() {
	r.stopped <- struct{}{}
	<-r.done
}

func (r *raftNode) onStop() {
	r.Stop()
	r.ticker.Stop()
	r.transport.Stop()
	if err := r.storage.Close(); err != nil {
		r.lg.Panic("failed to close Raft storage", zap.Error(err))
	}
	close(r.done)
}

// for testing
func (r *raftNode) pauseSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Pause()
}

func (r *raftNode) resumeSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Resume()
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}
