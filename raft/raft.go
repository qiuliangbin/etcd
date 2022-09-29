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
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/confchange"
	"go.etcd.io/etcd/raft/v3/quorum"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// 当前raft节点ID
	ID uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	// MaxSizePerMsg limits the max byte size of each append message. Smaller
	// value lowers the raft recovery cost(initial probing and message lost
	// during normal operation). On the other side, it might affect the
	// throughput during normal replication. Note: math.MaxUint64 for unlimited,
	// 0 for at most one entry per message.
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applied.
	MaxCommittedSizePerReady uint64
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an inaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = getLogger()
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type raft struct {
	// 当前节点在集群中的ID
	id uint64
	// 当前任期号
	Term uint64
	// 当前任期中当前节点将选票投给了哪个节点
	Vote uint64
	// 与只读请求相关
	readStates []ReadState

	// 每个节点的本地日志log
	raftLog *raftLog
	// 单条消息的最大字节数
	maxMsgSize uint64
	// 未提交Entry记录的最大值
	maxUncommittedSize uint64
	// TODO(tbg): rename to trk.
	prs tracker.ProgressTracker
	// 	当前节点在集群中的角色; Follower(跟随者), Candidate(候选者), Leader(领导者), PreCandidate(准备候选者),
	state StateType

	// isLearner is true if the local raft node is a learner.
	isLearner bool // 注：https://www.modb.pro/db/365678
	// 缓存了当前节点等待发送的消息
	msgs []pb.Message // 要发给其他节点的信息都存在这里，以后由上层channel取走

	// 当前集群中 Leader 节点的ID
	lead uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	// 用于集群中 Leader 节点的转移， leadTransferee 记录了此次 Leader 角色转移的目标节点的 ID
	leadTransferee uint64
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pendingConfIndex uint64
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	// 未提交的最大长度
	uncommittedSize uint64
	// 与只读请求相关
	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	// 选举计时器的指针，其单位是逻辑时钟的刻度，逻辑时钟每推进一次，该字段值就会增加 1
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// 心跳计时器的指针，其单位也是逻辑时钟的刻度，逻辑时钟每推进一次，该字段值就会增加 1
	heartbeatElapsed int
	// 用来防止网络分区产生的多主问题;
	// 如果一个follower在checkQuorum =true，并且已经有一个主了，并且自己并没有产生选举超时，
	// 那么这时候如果有别的节点给我发心跳包，我是直接忽视的
	checkQuorum bool
	// 打开pre-vote,主要是解决Candidate的网络分区问题,防止任期无限增加
	// 注: preCandidate是在真正参与选举之前，参选者会向集群中的所有结点先发送一条语句：我要选举啦，你们听到了吗？
	// 如果这时候有半数以上节点回应，则该节点就成为Candidate，否则变回follower
	// https://zhuanlan.zhihu.com/p/451436454
	preVote bool
	// 心跳计时器的指针，其单位也是逻辑时钟的刻度，逻辑时钟每推进一次，该字段值就会增加 1
	heartbeatTimeout int
	// 选举超时时间，当 electionElapsed 宇段值到达该值时， 就会触发新一轮的选举。
	electionTimeout int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	//  electiontimeout～2 × electiontimeout-1 之间的随机值，也是选举计时器的上限，当 electionElapsed 超过该值时即为超时。
	randomizedElectionTimeout int
	disableProposalForwarding bool
	// 当前节点推进逻辑时钟的函数。
	// 如果当前节点是 Leader, 则指向raft.tickH由此 beat() 函数;
	// 如果当前节点是 Follower 或是 Candidate, 则指向 raft.tickElection() 函数
	tick func()
	// 当前节点收到消息时的处理函数。
	// 如果是 Leader 节点， 则该字段指向 stepLeader() 函数，
	// 如果是 Follower 节点，则该字段指向 stepFollower() 函数，
	// 如果是处于 preVote 阶段的节点或是 Candidate 节点, 则该字段指向 stepCandidate() 函数
	step stepFunc

	logger Logger

	// pendingReadIndexMessages is used to store messages of type MsgReadIndex
	// that can't be answered as new leader didn't committed any log in
	// current term. Those will be handled as fast as first log is committed in
	// current term.
	pendingReadIndexMessages []pb.Message
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLogWithSize(c.Storage, c.Logger, c.MaxCommittedSizePerReady)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}

	r := &raft{
		id:                        c.ID,
		lead:                      None,
		isLearner:                 false,
		raftLog:                   raftlog,
		maxMsgSize:                c.MaxSizePerMsg,
		maxUncommittedSize:        c.MaxUncommittedEntriesSize,
		prs:                       tracker.MakeProgressTracker(c.MaxInflightMsgs),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,
		preVote:                   c.PreVote,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	}

	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: raftlog.lastIndex(),
	}, cs)
	if err != nil {
		panic(err)
	}
	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.prs.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// send schedules persisting state to a stable storage and AFTER that
// sending the message (as part of next Ready message processing).
func (r *raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	// 判断是否可以向指定的Follower节点发送消息, 创建Message实例
	pr := r.prs.Progress[to]
	if pr.IsPaused() {
		return false
	}
	m := pb.Message{}
	m.To = to
	// 查找待发送的消息
	term, errt := r.raftLog.term(pr.Next - 1)              // 拿到Peer Next-1位置在Leader raftLog中的任期
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize) // 获取待发送的Entry记录,一次最多发maxMsgSize条
	if len(ents) == 0 && !sendIfEmpty {                    // 合法性检测
		return false
	}
	// 如果在上述raftLog的查找出现异常,说明Peer的复制进度太落后(例如:网络脑裂、Peer节点宕机等),则会尝试发送MsgSnap消息
	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive { // 从当前集群的Leader节点的角度来看, 目标Follower节点已经不再存活
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		m.Type = pb.MsgSnap                   // 1.设置MsgSnap消息的Type字段
		snapshot, err := r.raftLog.snapshot() // 获取快照数据
		if err != nil {
			// 异常检测, 则终止整个程序
			if err == ErrSnapshotTemporarilyUnavailable { // 快照文件被删除、移动或者损坏会触发异常
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) { // 快照文件没内容也触发终止程序
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot                                            // 2.设置MsgSnap消息的Snapshot字段
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term //获取快照的关键信息(任期和索引)
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		// 将对应的Peer节点的状态设置成StateSnapshot, 其中会用Progress.PendingSnapshot字段记录快照数据的信息
		pr.BecomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		m.Type = pb.MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			switch pr.State {
			// optimistically increase the next when in StateReplicate
			case tracker.StateReplicate:
				last := m.Entries[n-1].Index
				pr.OptimisticUpdate(last)
				pr.Inflights.Add(last)
			case tracker.StateProbe:
				pr.ProbeSent = true
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	// 发送组装好的上述消息
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// sendHeartbeat 向指定节点发送消息
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	// 由于集群中的节点不一定都收到全部已提交的Entry记录，所以心跳消息携带commit提交位置
	commit := min(r.prs.Progress[to].Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat, // 消息类型为MsgHeartbeat
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
// bcastHeartbeat 广播发送心跳
func (r *raft) bcastHeartbeat() {
	// 如果readOnly中有只读消息，会在消息的上下文中携带readOnly中最后一条消息的消息ID，Follower节点收到后会在上下文中携带该消息ID响应
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

// bcastHeartbeatWithCtx 遍历向集群中的其他节点发送心跳消息
func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {

	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id { //过滤当前节点
			return
		}
		r.sendHeartbeat(id, ctx) // 向指定的节点发送MsgBeat消息
	})
}

func (r *raft) advance(rd Ready) {
	r.reduceUncommittedSize(rd.CommittedEntries)

	// If entries were applied (or a snapshot), update our cursor for
	// the next Ready. Note that if the current HardState contains a
	// new Commit index, this does not mean that we're also applying
	// all of the new entries due to commit pagination by size.
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		oldApplied := r.raftLog.applied
		r.raftLog.appliedTo(newApplied)

		if r.prs.Config.AutoLeave && oldApplied <= r.pendingConfIndex && newApplied >= r.pendingConfIndex && r.state == StateLeader {
			// If the current (and most recent, at least for this leader's term)
			// configuration should be auto-left, initiate that now. We use a
			// nil Data which unmarshals into an empty ConfChangeV2 and has the
			// benefit that appendEntry can never refuse it based on its size
			// (which registers as zero).
			ent := pb.Entry{
				Type: pb.EntryConfChangeV2,
				Data: nil,
			}
			// There's no way in which this proposal should be able to be rejected.
			if !r.appendEntry(ent) {
				panic("refused un-refusable auto-leaving ConfChangeV2")
			}
			r.pendingConfIndex = r.raftLog.lastIndex()
			r.logger.Infof("initiating automatic transition out of joint configuration %s", r.prs.Config)
		}
	}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}
	if !IsEmptySnap(rd.Snapshot) {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *raft) maybeCommit() bool {
	mci := r.prs.Committed()
	// 更新raftLog.committed字段，完成提交
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		// 重置Term
		r.Term = term
		// 重置Vote
		r.Vote = None
	}
	// 清空Lead字段
	r.lead = None
	// 重置选举计时器和心跳计时器
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 重置选举计时器的过期时间
	r.resetRandomizedElectionTimeout()
	// 清空leadTransferee
	r.abortLeaderTransfer()
	// 重置prs,其中每个Progress中的Next设置为raftLog.lastIndex()+1
	r.prs.ResetVotes()
	r.prs.Visit(func(id uint64, pr *tracker.Progress) {
		*pr = tracker.Progress{
			Match:     0,
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: tracker.NewInflights(r.prs.MaxInflight),
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			// 将当前节点对应的prs.Match设置成lastIndex()
			pr.Match = r.raftLog.lastIndex()
		}
	})

	r.pendingConfIndex = 0
	r.uncommittedSize = 0
	// 只读请求的相关设置
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...pb.Entry) (accepted bool) {
	// 获取raftLog中最后一条记录的索引值
	li := r.raftLog.lastIndex()
	// 更新待追加记录的Term值和索引值
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	// Track the size of this uncommitted proposal.
	if !r.increaseUncommittedSize(es) {
		r.logger.Debugf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
			r.id,
		)
		// Drop the proposal.
		return false
	}
	// use latest "last" index after truncate/append
	// 向raftLog中追加记录
	li = r.raftLog.append(es...)
	// 更新当前节点对应的Progress, 主要是更新Next和Match,用来标识对应节点的Entry记录复制的情况
	r.prs.Progress[r.id].MaybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	// 尝试提交Entry记录
	r.maybeCommit()
	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	// 递增electionElapsed选举计时器
	r.electionElapsed++
	// promotable 检查节点状态是否正常
	// pastElectionTimeout 检查选举计时器是否超时
	if r.promotable() && r.pastElectionTimeout() { //符合重新选举的条件
		// 重置选举计时器
		r.electionElapsed = 0
		// 发起选举~~~
		if err := r.Step(pb.Message{From: r.id, Type: pb.MsgHup}); err != nil {
			r.logger.Debugf("error occurred during election: %v", err)
		}
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
// tickHeartbeat 用于心跳未超时前，Leader会发送MsgBeat消息包用于探活
func (r *raft) tickHeartbeat() {
	// 递增心跳计时器
	r.heartbeatElapsed++
	// 递增选举计时器
	r.electionElapsed++
	// 选举超时
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0 // 重置选举计时器，Leader节点不会主动发起选举
		if r.checkQuorum {    // 进行多数检测
			// 当选举计时器超过electionTimeout时，会触发一次checkQuorum操作,
			// 该操作不会发送网络消息(虽然这里会通过raft.Step()方法处理消息)，
			// 它只是检查当前节点是否与集群中的大部分节点连通
			if err := r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum}); err != nil {
				r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
			}
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		// 选举计时器处于 electionTimeout~randomizedElectionTimeout 时间段时，不能进行Leader节点的转移
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader { // 检测当前是否为Leader节点
		return
	}
	// 如果当前节点为Leader节点，检测是否心跳计时器是否超时
	if r.heartbeatElapsed >= r.heartbeatTimeout { // 心跳计时器超时
		r.heartbeatElapsed = 0 // 重置心跳计时器
		// 发送MsgBeat消息
		if err := r.Step(pb.Message{From: r.id, Type: pb.MsgBeat}); err != nil {
			r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
		}
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	// 将step字段设置成stepFollower,stepFollower()函数中封装了Follower节点处理消息的行为
	r.step = stepFollower
	// 重置raft实例Term、Vote等字段
	r.reset(term)
	// 将tick字段设置成tickElection函数
	r.tick = r.tickElection
	// 设置当前集群的Leader节点
	r.lead = lead
	// 设置当前节点的角色
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate 当节点可以连接到集群中半数以上的节点时,会调用becomeCandidate()方法
// 切换到Candidate状态
func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	// 检测当前节点的状态,禁止直接从Leader状态切换到Candidate状态
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	// 将step字段设置成stepCandidate,stepCandidate()函数中封装了Candidate节点处理消息的行为
	r.step = stepCandidate
	// 重置raft实例Term、Vote等字段,任期Term+1
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	// 投票给自己
	r.Vote = r.id
	// 修改当前节点的角色为Candidate(候选者)
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	// 检查当前的节点的状态,禁止直接从Leader状态切换到PreCandidate
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	// 将step字段设置成stepCandidate,stepCandidate()函数中封装了PreCandidate节点处理消息的行为
	r.step = stepCandidate
	r.prs.ResetVotes()
	r.tick = r.tickElection
	r.lead = None
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

// becomeLeader 当Candidate节点得到集群中半数以上节点的选票时,
// 会调用becomeLeader()方法切换成Leader状态
func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	// 检查当前节点的状态, 禁止直接从Follower转换成Leader状态
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	// 将step字段设置成stepLeader,stepLeader()函数中封装了Leader节点处理消息的行为
	r.step = stepLeader
	// 重置raft实例Term/Vote等字段
	r.reset(r.Term)
	// 将tick字段设置成tickHeartbeat函数
	r.tick = r.tickHeartbeat
	// lead字段设置成当前节点id
	r.lead = r.id
	// 更新当前节点的角色为Leader
	r.state = StateLeader
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	r.prs.Progress[r.id].BecomeReplicate()

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.pendingConfIndex = r.raftLog.lastIndex()
	// 向当前节点的raftLog中追加一条空的Entry记录
	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	// As a special case, don't count the initial empty entry towards the
	// uncommitted log quota. This is because we want to preserve the
	// behavior of allowing one entry larger than quota if the current
	// usage is zero.
	// 提交未提交的日志(上一个Leader，已经复制到多数节点,而未提交的日志；通过发个空包进行提交)
	r.reduceUncommittedSize([]pb.Entry{emptyEnt})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *raft) hup(t CampaignType) {
	if r.state == StateLeader {
		r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}

	if !r.promotable() {
		r.logger.Warningf("%x is unpromotable and can not campaign", r.id)
		return
	}
	ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
	}
	if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
		r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
		return
	}

	r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
	r.campaign(t)
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
func (r *raft) campaign(t CampaignType) {
	if !r.promotable() {
		// This path should not be hit (callers are supposed to check), but
		// better safe than sorry.
		r.logger.Warningf("%x is unpromotable; campaign() should have been called", r.id)
	}
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}
	if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == quorum.VoteWon {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}
	var ids []uint64
	{
		idMap := r.prs.Voters.IDs()
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	for _, id := range ids {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
	}
}

func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.prs.RecordVote(id, v)
	return r.prs.TallyVotes()
}

// Step raft协议中的核心函数, 负责状态机的跳转.
// 	主要有2个逻辑:
// 		1.根据传进来的term和本身的term的大小, 决定要做的动作, 具体的逻辑原理可见raft协议原理;
// 		2.根据消息类型m.Type做不同的处理
func (r *raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
		}
		switch {
		case m.Type == pb.MsgPreVote:
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}

	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election. This can be prevented with Pre-Vote phase.
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else if m.Type == pb.MsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: pb.MsgPreVoteResp, Reject: true})
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type {
	case pb.MsgHup:
		if r.preVote {
			r.hup(campaignPreElection)
		} else {
			r.hup(campaignElection)
		}

	case pb.MsgVote, pb.MsgPreVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.lead == None) ||
			// ...or this is a PreVote for a future term...
			(m.Type == pb.MsgPreVote && m.Term > r.Term)
		// ...and we believe the candidate is up to date.
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			// Note: it turns out that that learners must be allowed to cast votes.
			// This seems counter- intuitive but is necessary in the situation in which
			// a learner has been promoted (i.e. is now a voter) but has not learned
			// about this yet.
			// For example, consider a group in which id=1 is a learner and id=2 and
			// id=3 are voters. A configuration change promoting 1 can be committed on
			// the quorum `{2,3}` without the config change being appended to the
			// learner's log. If the leader (say 2) fails, there are de facto two
			// voters remaining. Only 3 can win an election (due to its log containing
			// all committed entries), but to do so it will need 1 to vote. But 1
			// considers itself a learner and will continue to do so until 3 has
			// stepped up as leader, replicates the conf change to 1, and 1 applies it.
			// Ultimately, by receiving a request to vote, the learner realizes that
			// the candidate believes it to be a voter, and that it should act
			// accordingly. The candidate's config may be stale, too; but in that case
			// it won't win the election, at least in the absence of the bug discussed
			// in:
			// https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why, consider the
			// case where a single node was previously partitioned away and
			// it's local term is now out of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		// Propose消息类型走此道
		// step()是一个类似函数借口的东西,根据节点的类型不同而调用不同的函数,比如leader节点该函数就是raft.stepLeader()
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message) error

func stepLeader(r *raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat: // MsgBeat消息处理
		r.bcastHeartbeat() // 向所有节点广播心跳包(不携带任何其他数据)
		return nil
	case pb.MsgCheckQuorum: // MsgCheckQuorum消息处理
		// The leader should always see itself as active. As a precaution, handle
		// the case in which the leader isn't in the configuration any more (for
		// example if it just removed itself).
		//
		// TODO(tbg): I added a TODO in removeNode, it doesn't seem that the
		// leader steps down when removing itself. I might be missing something.
		if pr := r.prs.Progress[r.id]; pr != nil {
			pr.RecentActive = true
		}
		// 检测当前Leader节点是否与集群中大部分节点连通，如果不连通，则切换成Follower状态
		if !r.prs.QuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		// Mark everyone (but ourselves) as inactive in preparation for the next CheckQuorum.
		// 将每个人（除了我们自己）标记为不活动以准备下一次 CheckQuorum
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			if id != r.id {
				pr.RecentActive = false
			}
		})
		return nil
	case pb.MsgProp: // 客户端发往到集群的写请求是通过MsgProp消息表示的
		// 检测MsgProp消息是否携带了Entry记录, 如果未携带,则输出异常日志并终止程序
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		// 检测当前节点是否被移除集群, 如果当前节点以Leader状态被移除集群，则不再处理MsgProp消息
		if r.prs.Progress[r.id] == nil {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}
		// 检测当前是否正在进行Leader节点的转移,不再处理MsgProp消息
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}
		// 遍历MsgProp消息携带的全部Entry记录
		for i := range m.Entries {
			e := &m.Entries[i]
			// 参考链接: https://mrcroxx.github.io/posts/code-reading/etcdraft-made-simple/5-confchange/#32-%E6%8F%90%E8%AE%AEconfchange
			var cc pb.ConfChangeI
			// 反序列化ConfChange或者ConfChangeV2消息数据并对其进行一些预处理
			if e.Type == pb.EntryConfChange { // 旧版本的配置变更消息ConfChange仅支持“one node a time”的简单算法
				var ccc pb.ConfChange
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			} else if e.Type == pb.EntryConfChangeV2 {
				var ccc pb.ConfChangeV2
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			}
			if cc != nil {
				// alreadyPending：上一次合法的ConfChange还没被应用时为真
				alreadyPending := r.pendingConfIndex > r.raftLog.applied
				// alreadyJoint：当前配置正处于joint configuration时为真。
				alreadyJoint := len(r.prs.Config.Voters[1]) > 0
				// wantsLeaveJoint：如果消息（旧格式的消息会转为V2处理）的Changes字段为空时，说明该消息为用于退出joint configuration而转到$C_{new}$的消息
				wantsLeaveJoint := len(cc.AsV2().Changes) == 0

				var refused string
				if alreadyPending {
					// Raft同一时间只能有一个未被提交的ConfChange，因此拒绝新提议
					refused = fmt.Sprintf("possible unapplied conf change at index %d (applied to %d)", r.pendingConfIndex, r.raftLog.applied)
				} else if alreadyJoint && !wantsLeaveJoint {
					// 处于joint configuration的集群必须先退出joint configuration并转为$C_{new}，才能开始新的ConfChange，因此拒绝提议。
					refused = "must transition out of joint config first"
				} else if !alreadyJoint && wantsLeaveJoint {
					// 未处于joint configuration，忽略不做任何变化空ConfChange消息
					refused = "not in joint state; refusing empty conf change"
				}

				if refused != "" {
					// 对需要拒绝的提议的处理非常简单，只需要将该日志条目替换为没有任何意义的普通空日志条目pb.Entry{Type: pb.EntryNormal}即可
					r.logger.Infof("%x ignoring conf change %v at config %s: %s", r.id, cc, r.prs.Config, refused)
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else {
					// 而对于合法的ConfChange，除了将其追加到日志中外，还需要修改raft结构体的pendingConfIndex字段，
					// 将其置为$[上一条ConfChange.Index, 当前ConfChange.Index)$的值（这里置为了处理该MsgProp之前的最后一条日志的index），
					// 以供之后遇到ConfChange消息时判断当前ConfChange是否已经被应用。
					r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
				}
			}
		}
		// 将上述Entry记录追加到当前节点的raftLog中
		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		}
		// 通过MsgApp消息向集群中其他节点复制Entry记录
		r.bcastAppend()
		return nil
	case pb.MsgReadIndex:
		// 当 Leader 节点收到客户端的只读请求时，会将当前请求的编号记录下来，
		// 在返回数据给客户端之前，Leader节点需要先确定自己是否依然是当前集群的Leader节点(通过心跳的方式),
		// 在确定其依然是Leader节点之后，就可以说明该节点可以响应该请求，只需要等待当前Leader节点的提交位置
		// (即raftLog.committed)到达或是超过只读请求的编号即可向客户端返回响应。
		// only one voting member (the leader) in the cluster
		if r.prs.IsSingleton() { // 单机情况，直接回复客户端
			if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.To != None {
				r.send(resp)
			}
			return nil
		}

		// Postpone read only request when this leader has not committed
		// any log entry at its term.
		// 解决新leader当选时commit index落后的问题。如果leader在当前term还没提交过消息，则其会忽略该MsgReadIndex消息
		// 实现ReadIndex时存在一个特殊情况。当新leader刚刚当选时，其commit index可能并不是此时集群的commit index。
		// 因此，需要等到新leader至少提交了一条日志时，才能保证其commit index能反映集群此时的commit index。
		// 幸运的是，新leader当选时为了提交非本term的日志，会提交一条空日志。
		// 因此，leader只需要等待该日志提交就能开始提供ReadIndex服务，而无需再提交额外的空日志
		if !r.committedEntryInCurrentTerm() {
			r.pendingReadIndexMessages = append(r.pendingReadIndexMessages, m)
			return nil
		}

		sendMsgReadIndexResponse(r, m)

		return nil
	}

	// All other message types require a progress for m.From (pr).
	pr := r.prs.Progress[m.From]
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.Type {
	case pb.MsgAppResp:
		pr.RecentActive = true
		if m.Reject {
			// RejectHint is the suggested next base entry for appending (i.e.
			// we try to append entry RejectHint+1 next), and LogTerm is the
			// term that the follower has at index RejectHint. Older versions
			// of this library did not populate LogTerm for rejections and it
			// is zero for followers with an empty log.
			//
			// Under normal circumstances, the leader's log is longer than the
			// follower's and the follower's log is a prefix of the leader's
			// (i.e. there is no divergent uncommitted suffix of the log on the
			// follower). In that case, the first probe reveals where the
			// follower's log ends (RejectHint=follower's last index) and the
			// subsequent probe succeeds.
			//
			// However, when networks are partitioned or systems overloaded,
			// large divergent log tails can occur. The naive attempt, probing
			// entry by entry in decreasing order, will be the product of the
			// length of the diverging tails and the network round-trip latency,
			// which can easily result in hours of time spent probing and can
			// even cause outright outages. The probes are thus optimized as
			// described below.
			r.logger.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d",
				r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
			nextProbeIdx := m.RejectHint
			if m.LogTerm > 0 {
				// If the follower has an uncommitted log tail, we would end up
				// probing one by one until we hit the common prefix.
				//
				// For example, if the leader has:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 5 5 5 5 5
				//   term (F)   1 1 1 1 2 2
				//
				// Then, after sending an append anchored at (idx=9,term=5) we
				// would receive a RejectHint of 6 and LogTerm of 2. Without the
				// code below, we would try an append at index 6, which would
				// fail again.
				//
				// However, looking only at what the leader knows about its own
				// log and the rejection hint, it is clear that a probe at index
				// 6, 5, 4, 3, and 2 must fail as well:
				//
				// For all of these indexes, the leader's log term is larger than
				// the rejection's log term. If a probe at one of these indexes
				// succeeded, its log term at that index would match the leader's,
				// i.e. 3 or 5 in this example. But the follower already told the
				// leader that it is still at term 2 at index 6, and since the
				// log term only ever goes up (within a log), this is a contradiction.
				//
				// At index 1, however, the leader can draw no such conclusion,
				// as its term 1 is not larger than the term 2 from the
				// follower's rejection. We thus probe at 1, which will succeed
				// in this example. In general, with this approach we probe at
				// most once per term found in the leader's log.
				//
				// There is a similar mechanism on the follower (implemented in
				// handleAppendEntries via a call to findConflictByTerm) that is
				// useful if the follower has a large divergent uncommitted log
				// tail[1], as in this example:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 3 3 3 3 7
				//   term (F)   1 3 3 4 4 5 5 5 6
				//
				// Naively, the leader would probe at idx=9, receive a rejection
				// revealing the log term of 6 at the follower. Since the leader's
				// term at the previous index is already smaller than 6, the leader-
				// side optimization discussed above is ineffective. The leader thus
				// probes at index 8 and, naively, receives a rejection for the same
				// index and log term 5. Again, the leader optimization does not improve
				// over linear probing as term 5 is above the leader's term 3 for that
				// and many preceding indexes; the leader would have to probe linearly
				// until it would finally hit index 3, where the probe would succeed.
				//
				// Instead, we apply a similar optimization on the follower. When the
				// follower receives the probe at index 8 (log term 3), it concludes
				// that all of the leader's log preceding that index has log terms of
				// 3 or below. The largest index in the follower's log with a log term
				// of 3 or below is index 3. The follower will thus return a rejection
				// for index=3, log term=3 instead. The leader's next probe will then
				// succeed at that index.
				//
				// [1]: more precisely, if the log terms in the large uncommitted
				// tail on the follower are larger than the leader's. At first,
				// it may seem unintuitive that a follower could even have such
				// a large tail, but it can happen:
				//
				// 1. Leader appends (but does not commit) entries 2 and 3, crashes.
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 2 2     [crashes]
				//   term (F)   1
				//   term (F)   1
				//
				// 2. a follower becomes leader and appends entries at term 3.
				//              -----------------
				//   term (x)   1 2 2     [down]
				//   term (F)   1 3 3 3 3
				//   term (F)   1
				//
				// 3. term 3 leader goes down, term 2 leader returns as term 4
				//    leader. It commits the log & entries at term 4.
				//
				//              -----------------
				//   term (L)   1 2 2 2
				//   term (x)   1 3 3 3 3 [down]
				//   term (F)   1
				//              -----------------
				//   term (L)   1 2 2 2 4 4 4
				//   term (F)   1 3 3 3 3 [gets probed]
				//   term (F)   1 2 2 2 4 4 4
				//
				// 4. the leader will now probe the returning follower at index
				//    7, the rejection points it at the end of the follower's log
				//    which is at a higher log term than the actually committed
				//    log.
				nextProbeIdx = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == tracker.StateReplicate {
					pr.BecomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == tracker.StateProbe:
					pr.BecomeReplicate()
				case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot:
					// TODO(tbg): we should also enter this branch if a snapshot is
					// received that is below pr.PendingSnapshot but which makes it
					// possible to use the log again.
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					pr.BecomeProbe()
					pr.BecomeReplicate()
				case pr.State == tracker.StateReplicate:
					pr.Inflights.FreeLE(m.Index)
				}

				if r.maybeCommit() {
					// committed index has progressed for the term, so it is safe
					// to respond to pending read index requests
					releasePendingReadIndexMessages(r)
					r.bcastAppend()
				} else if oldPaused {
					// If we were paused before, this node may be missing the
					// latest commit index, so send it.
					r.sendAppend(m.From)
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				for r.maybeSendAppend(m.From, false) {
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		pr.RecentActive = true // 更新对应Progress.RecentActive，表示Follower节点与自己连通
		pr.ProbeSent = false   // 重置Progress.Paused字段,表示可以继续向Follower节点发送消息

		// free one slot for the full inflights window to allow progress.
		if pr.State == tracker.StateReplicate && pr.Inflights.Full() {
			pr.Inflights.FreeFirstOne() // 释放inflights中第一个消息, 这样就可以开始后续消息的发送
		}
		// 当Leader节点收到Follower节点的MsgHeartbeatResp之后,
		// 会比较对应的Match值与Leader节点的raftLog,从而判断Follower节点是否已拥有了全部的Entry记录
		// 如果该Follower缺少Entry，则Leader会发送缺少的Entry记录
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From) // 通过向指定节点发送MsgApp消息完成Entry记录的复制
		}
		// 如果该节点的只读模式不是ReadOnlySafe，则返回
		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}
		// 统计目前为止响应上述携带消息ID的MsgHeartbeat消息的节点个数，如果没有超过半数则退出
		if r.prs.Voters.VoteResult(r.readOnly.recvAck(m.From, m.Context)) != quorum.VoteWon {
			return nil
		}
		// 响应节点超过半数之后，会清空readOnly中指定消息ID及其之前的所有相关记录
		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			// 回复ReadIndexReq包请求
			if resp := r.responseToReadIndexReq(rs.req, rs.index); resp.To != None {
				r.send(resp)
			}
		}
	case pb.MsgSnapStatus:
		if pr.State != tracker.StateSnapshot {
			return nil
		}
		// TODO(tbg): this code is very similar to the snapshot handling in
		// MsgAppResp above. In fact, the code there is more correct than the
		// code here and should likely be updated to match (or even better, the
		// logic pulled into a newly created Progress state machine handler).
		if !m.Reject {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			pr.PendingSnapshot = 0
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the MsgAppResp from the remote node before sending
		// out the next MsgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.ProbeSent = true
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == tracker.StateReplicate {
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) error {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)
	case myVoteRespType:
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		switch res {
		case quorum.VoteWon:
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case quorum.VoteLost:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
	return nil
}

func stepFollower(r *raft, m pb.Message) error {
	switch m.Type {
	case pb.MsgProp:
		if r.lead == None { // 当前集群中没有Leader节点，则忽略该MsgProp消息
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding { // Leader节点被移除出集群, 则返回ErrProposalDropped
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return ErrProposalDropped
		}
		// 将消息的To字段设置为当前集群的Leader节点id
		m.To = r.lead
		// 将MsgProp消息转发给当前集群的Leader节点
		r.send(m)
	case pb.MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat: // MsgHeartbeat消息处理
		r.electionElapsed = 0 // 重置选举计时器，防止当前Follower节点发起新一轮选举
		r.lead = m.From       // 设置Leader节点的ID
		// 其中会修改raft.committed字段的值(放心，在Leader发送消息时，已经确定了当前Follower节点
		// 有committed之前的全部日志记录)，然后发送MsgHeartbeatResp类型消息，响应此次心跳
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.electionElapsed = 0 // 重置raft.electionElapsed, 防止发生选举
		r.lead = m.From       // 设置Leader的id
		r.handleSnapshot(m)   // 通过MsgSnap消息中的快照书籍, 重建当前节点的raftLog
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
		// Leadership transfers never use pre-vote even if r.preVote is true; we
		// know we are not recovering from a partition so there is no need for the
		// extra round trip.
		r.hup(campaignTransfer)
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		// 目标为leader
		m.To = r.lead
		// 将MsgReadIndex消息转发给当前集群的Leader节点
		r.send(m)
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 { // 检测MsgReadIndexResp消息的合法性
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		// 将MsgReadIndex消息对应的消息ID以及已提交位置(raftLog.committed)封装成ReadState实例,
		// 并添加到raft.readStates中, 等待其他goroutine处理
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

func (r *raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)

		// Return a hint to the leader about the maximum index and term that the
		// two logs could be divergent at. Do this by searching through the
		// follower's log for the maximum (index, term) pair with a term <= the
		// MsgApp's LogTerm and an index <= the MsgApp's Index. This can help
		// skip all indexes in the follower's uncommitted tail with terms
		// greater than the MsgApp's LogTerm.
		//
		// See the other caller for findConflictByTerm (in stepLeader) for a much
		// more detailed explanation of this mechanism.
		hintIndex := min(m.Index, r.raftLog.lastIndex())
		hintIndex = r.raftLog.findConflictByTerm(hintIndex, m.LogTerm)
		hintTerm, err := r.raftLog.term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		}
		r.send(pb.Message{
			To:         m.From,
			Type:       pb.MsgAppResp,
			Index:      m.Index,
			Reject:     true,
			RejectHint: hintIndex,
			LogTerm:    hintTerm,
		})
	}
}

func (r *raft) handleHeartbeat(m pb.Message) {
	// 根据MsgHeartbeat消息的Commit字段，更新raftLog中记录的已提交位置;
	// 注意,在Leader节点发送MsgHeartbeat消息时,
	// 已经确认了当前Follower节点中raftLog.committed字段的合适位置
	r.raftLog.commitTo(m.Commit)
	// 发送MsgHeartbeatResp消息，响应心跳
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleSnapshot(m pb.Message) {
	// 获取快照数据的元数据
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) { // 返回值表示是否通过快照数据进行了重建raftLog
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		// 如果重建成功则返回MsgAppResp消息，Index为当前日志最新的索引值
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		// 如果重建失败则返回MsgAppResp消息，Index为当前日志已提交位置
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine. If this method returns false, the snapshot was
// ignored, either because it was obsolete or because of an error.
func (r *raft) restore(s pb.Snapshot) bool {
	// 如果快照中的Index小于当前raftLog已提交位置则返回false, 记录都已经存在，则直接返回不需要加载snapshot消息
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	if r.state != StateFollower {
		// This is defense-in-depth: if the leader somehow ended up applying a
		// snapshot, it could move into a new term without moving into a
		// follower state. This should never fire, but if it did, we'd have
		// prevented damage by returning early, so log only a loud warning.
		//
		// At the time of writing, the instance is guaranteed to be in follower
		// state when this method is called.
		r.logger.Warningf("%x attempted to restore snapshot as leader; should never happen", r.id)
		r.becomeFollower(r.Term+1, None)
		return false
	}

	// More defense-in-depth: throw away snapshot if recipient is not in the
	// config. This shouldn't ever happen (at the time of writing) but lots of
	// code here and there assumes that r.id is in the progress tracker.
	found := false
	cs := s.Metadata.ConfState
	// 该节点是否存在于snapshot快照元数据中;不存在,则拒绝加载快照
	for _, set := range [][]uint64{
		cs.Voters,
		cs.Learners,
		cs.VotersOutgoing,
		// `LearnersNext` doesn't need to be checked. According to the rules, if a peer in
		// `LearnersNext`, it has to be in `VotersOutgoing`.
	} {
		for _, id := range set {
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
		r.logger.Warningf(
			"%x attempted to restore snapshot but it is not in the ConfState %v; should never happen",
			r.id, cs,
		)
		return false
	}

	// Now go ahead and actually restore.
	// 根据快照书籍的元数据查找匹配的Entry记录,如果存在,则表示当前节点已经拥有了该快照中的全部数据,
	// 那么就无须进行后续的重建操作
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		// 快照中全部的Entry记录都已经提交了,所以尝试当前节点的raftLog.committed。
		// 根据raft协议可知, raftLog.committed只增不减
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}
	// 通过层层校验,可以开始重建raftLog了
	// 先将快照数据加载到raftLog.unstable中,同时重置相关字段
	r.raftLog.restore(s)

	// Reset the configuration and add the (potentially updated) peers in anew.
	r.prs = tracker.MakeProgressTracker(r.prs.MaxInflight)
	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: r.raftLog.lastIndex(),
	}, cs)

	if err != nil {
		// This should never happen. Either there's a bug in our config change
		// handling or the client corrupted the conf change.
		panic(fmt.Sprintf("unable to restore config %+v: %s", cs, err))
	}

	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))
	// 更新当前节点自身的Match值
	pr := r.prs.Progress[r.id]
	pr.MaybeUpdate(pr.Next - 1) // TODO(tbg): this is untested and likely unneeded

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	pr := r.prs.Progress[r.id]
	// 节点没有从集群中移除, 不是Leader, 没有待应用的snapshot
	return pr != nil && !pr.IsLearner && !r.raftLog.hasPendingSnapshot()
}

func (r *raft) applyConfChange(cc pb.ConfChangeV2) pb.ConfState {
	cfg, prs, err := func() (tracker.Config, tracker.ProgressMap, error) {
		changer := confchange.Changer{
			Tracker:   r.prs,
			LastIndex: r.raftLog.lastIndex(),
		}
		if cc.LeaveJoint() {
			return changer.LeaveJoint()
		} else if autoLeave, ok := cc.EnterJoint(); ok {
			return changer.EnterJoint(autoLeave, cc.Changes...)
		}
		return changer.Simple(cc.Changes...)
	}()

	if err != nil {
		// TODO(tbg): return the error to the caller.
		panic(err)
	}

	return r.switchToConfig(cfg, prs)
}

// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
func (r *raft) switchToConfig(cfg tracker.Config, prs tracker.ProgressMap) pb.ConfState {
	r.prs.Config = cfg
	r.prs.Progress = prs

	r.logger.Infof("%x switched to configuration %s", r.id, r.prs.Config)
	cs := r.prs.ConfState()
	pr, ok := r.prs.Progress[r.id]

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
	r.isLearner = ok && pr.IsLearner

	if (!ok || r.isLearner) && r.state == StateLeader {
		// This node is leader and was removed or demoted. We prevent demotions
		// at the time writing but hypothetically we handle them the same way as
		// removing the leader: stepping down into the next Term.
		//
		// TODO(tbg): step down (for sanity) and ask follower with largest Match
		// to TimeoutNow (to avoid interruption). This might still drop some
		// proposals but it's better than nothing.
		//
		// TODO(tbg): test this branch. It is untested at the time of writing.
		return cs
	}

	// The remaining steps only make sense if this node is the leader and there
	// are other nodes.
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if r.maybeCommit() {
		// If the configuration change means that more entries are committed now,
		// broadcast/append to everyone in the updated config.
		r.bcastAppend()
	} else {
		// Otherwise, still probe the newly added replicas; there's no reason to
		// let them wait out a heartbeat interval (or the next incoming
		// proposal).
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			r.maybeSendAppend(id, false /* sendIfEmpty */)
		})
	}
	// If the the leadTransferee was removed or demoted, abort the leadership transfer.
	if _, tOK := r.prs.Config.Voters.IDs()[r.leadTransferee]; !tOK && r.leadTransferee != 0 {
		r.abortLeaderTransfer()
	}

	return cs
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// committedEntryInCurrentTerm return true if the peer has committed an entry in its term.
func (r *raft) committedEntryInCurrentTerm() bool {
	return r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) == r.Term
}

// responseToReadIndexReq constructs a response for `req`. If `req` comes from the peer
// itself, a blank value will be returned.
func (r *raft) responseToReadIndexReq(req pb.Message, readIndex uint64) pb.Message {
	if req.From == None || req.From == r.id {
		// 如采是客户端直接发送到 Leader 节点的消息,则将 MsgReadindex 消息对应的已提交往置
		// 以及其消息 ID 封装成 ReadState 实例, 添加到 raft.readStates 中保存.
		// 后续会有其他 goroutine 读取该数组，并对相应的 MsgReadindex 消息进行响应
		r.readStates = append(r.readStates, ReadState{
			Index:      readIndex,
			RequestCtx: req.Entries[0].Data,
		})
		return pb.Message{}
	}
	// 如果是其他Follower节点转发到Leader节点的MsgReadIndex消息, 则Leader节点
	// 会向Follower节点返回相应的MsgReadIndexResp消息, 并由Follower节点响应Client
	return pb.Message{
		Type:    pb.MsgReadIndexResp,
		To:      req.From,
		Index:   readIndex,
		Entries: req.Entries,
	}
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
//
// Empty payloads are never refused. This is used both for appending an empty
// entry at a new leader's term, as well as leaving a joint configuration.
func (r *raft) increaseUncommittedSize(ents []pb.Entry) bool {
	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}

	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		// Note the added requirement s>0 which is used to make sure that
		// appending single empty entries to the log always succeeds, used both
		// for replicating a new leader's initial empty entry, and for
		// auto-leaving joint configurations.
		return false
	}
	r.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *raft) reduceUncommittedSize(ents []pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange || ents[i].Type == pb.EntryConfChangeV2 {
			n++
		}
	}
	return n
}

func releasePendingReadIndexMessages(r *raft) {
	if !r.committedEntryInCurrentTerm() {
		r.logger.Error("pending MsgReadIndex should be released only after first commit in current term")
		return
	}

	msgs := r.pendingReadIndexMessages
	r.pendingReadIndexMessages = nil

	for _, m := range msgs {
		sendMsgReadIndexResponse(r, m)
	}
}

func sendMsgReadIndexResponse(r *raft, m pb.Message) {
	// thinking: use an internally defined context instead of the user given context.
	// We can express this in terms of the term and index instead of a user-supplied value.
	// This would allow multiple reads to piggyback on the same message.
	switch r.readOnly.option {
	// If more than the local vote is needed, go through a full broadcast.
	case ReadOnlySafe:
		// 当仅使用ReadIndex时，leader会将当前的commit index作为read index并通过readOnly的addRequest方法将其加入到待确认的队列中。
		// 然后leader节点自己先确认该read index，然后广播心跳等待其它节点确认该read index。
		// leader在主动请求确认read index时，发送的心跳消息携带的rctx就是该read index相应的rctx；
		// 而当leader因heartbeat timeout超时而广播心跳消息时，携带的是待确认的最后一条read index相应的rctx，
		// 以批量确认所有待确认的read index

		// 记录当前节点的 raftLog.committed 字段值，即己提交的位置
		r.readOnly.addRequest(r.raftLog.committed, m)
		// leader节点自己先确认该read index
		r.readOnly.recvAck(r.id, m.Entries[0].Data)
		// 广播心跳等待其它节点确认该read index
		r.bcastHeartbeatWithCtx(m.Entries[0].Data)
	case ReadOnlyLeaseBased:
		// 当使用Lease Read时，leader可以直接返回相应的ReadState，因为etcd/raft的Lease Read是通过Check Quorum实现的。
		// 即只要leader没有退位，说明其仍持有lease；
		// 而当leader无法为lease续约时，Check Quorum机制会让leader退位为follower，其也就不会通过stepLeader方法处理MsgReadIndex请求
		if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.To != None {
			r.send(resp)
		}
	}
}
