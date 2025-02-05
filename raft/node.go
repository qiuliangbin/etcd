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

package raft

import (
	"context"
	"errors"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	// 当前集群的Leader节点ID
	Lead uint64 // must use atomic operations to access; keep 64-bit aligned.
	// 当前节点在集群中的角色
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
// Ready Ready实例只是用来传递数据的,其全部字段都是只读的
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	// 包含了当前集群的Leader节点ID(Lead字段)即当前节点的角色(RaftState字段)
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	// 封装了当前节点的任期号和当前节点的在该任期的投票结果,及当前节点的raftLog的已提交位置
	pb.HardState

	// ReadStates can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
	// ReadStates 记录了当前节点中等待处理的只读请求
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE Messages are sent.
	// Entries 指从unstable中读取出来的Entry记录，上层模块会将其保存到Storage中
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	// 待持久化的快照数据，raftpb.Snapshot中封装了快照数据和相关元数据
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// CommittedEntries 已提交，待应用的Entry记录, 这些Entry记录之前已经保存在Storage中
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	// Messages 保存了当前节点中等待发送到集群其他节点的Message消息
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	// MustSync 开启MustSync=true，则HardState和Entries两个字段的内容必须同步刷新到磁盘
	MustSync bool
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 || len(rd.ReadStates) != 0
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if index := rd.Snapshot.Metadata.Index; index > 0 {
		return index
	}
	return 0
}

// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	// 推进逻辑时钟的指针，包含[选举计时器]和[心跳计时器]两个计时器的时钟推进
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	// 当选举计时器超时之后，会调用Campaign()方法会将当前节点切换成Candidate状态(或者PreCandidate状态),
	// 底层就是通过MsgHub消息实现的,
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	// Client会发送读写请求时，Node实例会调用Propose()方法，底层就是通过发送Propose消息实现
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes a configuration change. Like any proposal, the
	// configuration change may be dropped with or without an error being
	// returned. In particular, configuration changes are dropped unless the
	// leader has certainty that there is no prior unapplied configuration
	// change in its log.
	//
	// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
	// message. The latter allows arbitrary configuration changes via joint
	// consensus, notably including replacing a voter. Passing a ConfChangeV2
	// message is only allowed if all Nodes participating in the cluster run a
	// version of this library aware of the V2 API. See pb.ConfChangeV2 for
	// usage details and semantics.
	// Client会发送修改集群配置请求时，Node实例会调用ProposeConfChange()方法，底层就是通过发送ProposeConfChange消息实现
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	// 当前节点收到其他节点的消息时，会通过Step()方法将消息交给底层封装的raft实例进行处理
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	// Ready()返回一个的Channel, Channel中封装了底层raft实例的相关状态数据。
	// 例如，需要发送到其他节点的消息和交给上层模块的Entry记录，等等。
	// 这是etcd-raft与上层模块交互的主要方式
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	// 当上层模块处理完上述Channel中返回的Ready实例之后, 需要调用Advance()通知底层etcd-raft模块返回新的Ready实例
	Advance()
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	//
	// Returns an opaque non-nil ConfState protobuf which must be recorded in
	// snapshots.
	// 在收到集群配置请求时, 会通过调用ApplyConfChange()方法进行处理
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	// 用于Leader节点的转移
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	// Note that request can be lost without notice, therefore it is user's job
	// to ensure read index retries.
	// 用于处理只读请求
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	// 返回当前节点的状态运行状态
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	// 通知底层的raft实例，当前节点无法与指定的节点进行通讯
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	// 用于通知底层的raft实例上次发送快照的结果
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	// 关闭当前节点
	Stop()
}

type Peer struct {
	ID      uint64
	Context []byte
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
// StartNode Config为传入配置信息, Peers封装了节点的ID, peers记录当前集群中全部节点的ID
func StartNode(c *Config, peers []Peer) Node {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	// 创建一个RawNode实例
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	// 启动RawNode实例
	err = rn.Bootstrap(peers)
	if err != nil {
		c.Logger.Warningf("error occurred during starting a new node: %v", err)
	}
	// 初始化node实例
	n := newNode(rn)
	// 启动一个goroutine, 其中会根据底层raft的状态及上层模块传递的数据, 协调处理node中各个通道的数据
	go n.run()
	return &n
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
func RestartNode(c *Config) Node {
	// 创建底层封装的raft实例, 该过程与StartNode()函数相同
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	// 创建node实例
	n := newNode(rn)
	// 启动一个goroutine, 其中会根据底层raft的状态及上层模块传递的数据,协调处理node中各种通道的数据
	go n.run()
	return &n
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

// node is the canonical implementation of the Node interface
type node struct {
	// 下述通道用于接受MsgProp类型的消息
	propc chan msgWithResult
	// 除MsgProp外其他类型的消息都是由该通道接收的
	recvc chan pb.Message
	// 当节点收到EntryConfChange类型的Entry记录时, 会转换成ConfChange, 并写入该通道中等待处理。
	// 在ConfChange中封装了其唯一ID,待处理的节点ID(NodeID字段)及处理类型(Type字段, 例如：
	// ConfChangeAddNode类型表示添加节点)等信息
	confc chan pb.ConfChangeV2
	// 在ConfState中封装了当前集群中所有节点的ID, 该通道用于向上层模块返回Ready实例,
	// 即node.Ready()方法的返回值
	confstatec chan pb.ConfState
	// readyc通道用于向上层模块返回Ready实例,即node.Ready()方法的返回值
	readyc chan Ready
	// 当上层模块处理完通过上述readyc通道获取到的Ready实例后,
	// 会通过node.Advance()方法向该通道写入信号, 从而通知底层raft实例
	advancec chan struct{}
	// 用来接收逻辑时钟(选举计时器和心跳计时器)发出的信号
	tickc chan struct{}
	// 当检测到done通道关闭后, 在其上阻塞的goroutine会继续执行,并进行相应的关闭操作
	done chan struct{}
	// 当node.Stop()方法被调用时, 会向该通道发送信号,
	// 并有另一个goroutine会尝试读取该通道中的内容, 当读取到信息之后, 会关闭done通道
	stop chan struct{}
	// 传递的元素为Channel类型,即node.Status()方法的返回值
	status chan chan Status

	rn *RawNode
}

func newNode(rn *RawNode) node {
	return node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChangeV2),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 128), // 带缓冲区的channel
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan Status),
		rn:     rn,
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

// run raft协议协程入口函数,就是EventLoop, 循环处理各个channel的事件,从而进行状态跳转
func (n *node) run() {
	var propc chan msgWithResult
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready

	r := n.rn.raft

	lead := None

	for {
		// readyc 和 advance 只有一个是有效值
		// 如果advancec是nil，说明刚commit了，可以创建ready channel来继续去把ready commit的commit了
		if advancec != nil {
			readyc = nil
		} else if n.rn.HasReady() {
			// Populate a Ready. Note that this Ready is not guaranteed to
			// actually be handled. We will arm readyc, but there's no guarantee
			// that we will actually send on it. It's possible that we will
			// service another channel instead, loop around, and then populate
			// the Ready again. We could instead force the previous Ready to be
			// handled first, but it's generally good to emit larger Readys plus
			// it simplifies testing (by emitting less frequently and more
			// predictably).
			rd = n.rn.readyWithoutAccept()
			readyc = n.readyc
		}

		if lead != r.lead {
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		// EtcdServer会往channel propc里面被塞入了Propose消息类型的数据; run()中从channel propc中读取Propose消息,并进行处理.
		case pm := <-propc:
			m := pm.m
			m.From = r.id
			// 处理从channel propc中读取Propose消息
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		case m := <-n.recvc:
			// filter out response message from unknown From.
			if pr := r.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				r.Step(m)
			}
		case cc := <-n.confc:
			_, okBefore := r.prs.Progress[r.id]
			cs := r.applyConfChange(cc)
			// If the node was removed, block incoming proposals. Note that we
			// only do this if the node was in the config before. Nodes may be
			// a member of the group without knowing this (when they're catching
			// up on the log and don't have the latest config) and we don't want
			// to block the proposal channel in that case.
			//
			// NB: propc is reset when the leader changes, which, if we learn
			// about it, sort of implies that we got readded, maybe? This isn't
			// very sound and likely has bugs.
			if _, okAfter := r.prs.Progress[r.id]; okBefore && !okAfter {
				var found bool
				for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
					for _, id := range sl {
						if id == r.id {
							found = true
							break
						}
					}
					if found {
						break
					}
				}
				if !found {
					propc = nil
				}
			}
			select {
			case n.confstatec <- cs:
			case <-n.done:
			}
		case <-n.tickc:
			n.rn.Tick()
		case readyc <- rd:
			n.rn.acceptReady(rd)
			advancec = n.advancec
		case <-advancec:
			n.rn.Advance(rd)
			rd = Ready{}
			advancec = nil
		case c := <-n.status:
			c <- getStatus(r)
		case <-n.stop:
			close(n.done)
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.rn.raft.logger.Warningf("%x A tick missed to fire. Node blocks too long!", n.rn.raft.id)
	}
}

func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

func (n *node) Propose(ctx context.Context, data []byte) error {
	// 消息类型为Propose
	return n.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	msg, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return n.Step(ctx, msg)
}

func (n *node) step(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *node) stepWait(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *node) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	// 消息类型不为Propose,则把传进来的pb.Message赋值给channel n.recvc
	if m.Type != pb.MsgProp {
		select {
		case n.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}
	// Propose消息类型包括: type RaftKV interface下的所有接口
	// 消息类型为Propose,则把传进来的pb.Message赋值给channel n.propc
	ch := n.propc
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	// 如果为Propose消息,将Propose消息塞入propc通道
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	select {
	// 阻塞等待raft的处理结果
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

func (n *node) Ready() <-chan Ready { return n.readyc }

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc.AsV2():
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) Status() Status {
	c := make(chan Status)
	select {
	case n.status <- c:
		return <-c
	case <-n.done:
		return Status{}
	}
}

func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
