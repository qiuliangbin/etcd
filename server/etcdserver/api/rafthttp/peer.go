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

package rafthttp

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	DefaultConnReadTimeout  = 5 * time.Second
	DefaultConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
	maxPendingProposals = 4096

	streamAppV2 = "streamMsgAppV2"
	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
	sendSnap    = "sendMsgSnap"
)

var (
	ConnReadTimeout  = DefaultConnReadTimeout
	ConnWriteTimeout = DefaultConnWriteTimeout
)

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	// 发送单个消息,该方法是非阻塞的,如果出现发送失败,则会将失败信息报告给底层的Raft接口
	send(m raftpb.Message)

	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	// 发送snap.Message,其他行为与send()方法类似
	sendSnap(m snap.Message)

	// update updates the urls of remote peer.
	// 更新对应节点暴露的URL地址
	update(urls types.URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	// 将指定的连接与当前Peer绑定,Peer会将该连接作为Stream消息通道使用,
	// 当Peer不再使用该连接时,会将该连接关闭
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	// 返回与对端建立连接的时间
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	// 关闭当前Peer实例,会关闭底层的网络连接
	stop()
}

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
type peer struct {
	lg *zap.Logger

	localID types.ID
	// id of the remote raft peer node
	// 该peer实例对应的节点的ID
	id types.ID
	// Raft接口,在Raft接口实现的底层封装了etcd-raft模块
	r Raft

	status *peerStatus
	// 每个节点可能提供了多个URL供其他节点访问,当其中一个访问失败时,我们应该可以尝试访问另一个。
	// 而urlPicker提供的主要功能就是在这些URL之间进行切换
	picker *urlPicker

	msgAppV2Writer *streamWriter
	// 负责向Stream消息通道写入消息
	writer *streamWriter
	// Pipeline消息通道
	pipeline *pipeline
	// 负责发送快照数据
	snapSender     *snapshotSender // snapshot sender to send v3 snapshot messages
	msgAppV2Reader *streamReader
	// 负责从Stream消息通道中读取消息
	msgAppReader *streamReader
	// 从Stream消息通道中读取到消息之后,会通过该通道将消息交给Raft接口,然后由它返回给底层etcd-raft模块进行处理
	recvc chan raftpb.Message
	// 从Stream消息通道中读取到MsgProp类型的消息之后,会通过Stream消息通道将MsgProp消息交给Raft接口,
	// 然后由它(propc)返回给底层etcd-raft模块进行处理
	propc chan raftpb.Message

	mu sync.Mutex
	// 是否暂停向对应节点发送消息
	paused bool

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func startPeer(t *Transport, urls types.URLs, peerID types.ID, fs *stats.FollowerStats) *peer {
	if t.Logger != nil {
		t.Logger.Info("starting remote peer", zap.String("remote-peer-id", peerID.String()))
	}
	defer func() {
		if t.Logger != nil {
			t.Logger.Info("started remote peer", zap.String("remote-peer-id", peerID.String()))
		}
	}()

	status := newPeerStatus(t.Logger, t.ID, peerID)
	picker := newURLPicker(urls) // 根据节点提供的URL创建urlPicker
	errorc := t.ErrorC
	r := t.Raft            // 底层的Raft状态机
	pipeline := &pipeline{ // 创建pipeline实例
		peerID:        peerID,
		tr:            t,
		picker:        picker,
		status:        status,
		followerStats: fs,
		raft:          r,
		errorc:        errorc,
	}
	pipeline.start() // 启动pipeline

	p := &peer{ // 创建Peer实例
		lg:             t.Logger,
		localID:        t.ID,
		id:             peerID,
		r:              r,
		status:         status,
		picker:         picker,
		msgAppV2Writer: startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		writer:         startStreamWriter(t.Logger, t.ID, peerID, status, fs, r), // 创建并启动streamWriter
		pipeline:       pipeline,
		snapSender:     newSnapshotSender(t, picker, peerID, status),
		recvc:          make(chan raftpb.Message, recvBufSize),         // 创建recvc通道, 缓冲区大小4096
		propc:          make(chan raftpb.Message, maxPendingProposals), // 创建propc通道,缓冲区大小4096
		stopc:          make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go func() {
		for {
			select {
			case mm := <-p.recvc: // 从recvc通道中获取来自底层网络连接发过来的Message
				if err := r.Process(ctx, mm); err != nil { // 将Message交给底层Raft状态机处理
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.
	// Thus propc must be put into a separate routine with recvc to avoid blocking
	// processing other raft messages.
	go func() {
		for {
			select {
			case mm := <-p.propc: // 从propc通道中获取MsgProp类型的Message
				if err := r.Process(ctx, mm); err != nil { // 将Message交给底层Raft状态机处理
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	p.msgAppV2Reader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMsgAppV2,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}
	// 创建streamReader实例
	p.msgAppReader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMessage,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}

	p.msgAppV2Reader.start()
	// 启动streamReader实例,它主要负责从Stream消息通道上读取消息
	p.msgAppReader.start()

	return p
}

// send 消息发送功能
func (p *peer) send(m raftpb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused { // 检查paused字段,是否暂停对指定节点发送消息
		return
	}
	// 根据消息的类型选择合适的消息通道
	writec, name := p.pick(m)
	select {
	case writec <- m: // 将Message写入writec(缓冲区大小为64)通道中,等到发送
	default:
		// 如果发送出现阻塞,则将信息报告给底层Raft状态机,这里会根据消息类型选择合适的报告方法
		p.r.ReportUnreachable(m.To)
		if isMsgSnap(m) {
			p.r.ReportSnapshot(m.To, raft.SnapshotFailure)
		}
		if p.lg != nil {
			p.lg.Warn( // 记录日志
				"dropped internal Raft message since sending buffer is full",
				zap.String("message-type", m.Type.String()),
				zap.String("local-member-id", p.localID.String()),
				zap.String("from", types.ID(m.From).String()),
				zap.String("remote-peer-id", p.id.String()),
				zap.String("remote-peer-name", name),
				zap.Bool("remote-peer-active", p.status.isActive()),
			)
		}
		sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
	}
}

func (p *peer) sendSnap(m snap.Message) {
	go p.snapSender.send(m)
}

func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}

func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMsgAppV2:
		ok = p.msgAppV2Writer.attach(conn)
	case streamTypeMessage:
		ok = p.writer.attach(conn)
	default:
		if p.lg != nil {
			p.lg.Panic("unknown stream type", zap.String("type", conn.t.String()))
		}
	}
	if !ok {
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
	p.msgAppV2Reader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
	p.msgAppV2Reader.resume()
}

func (p *peer) stop() {
	if p.lg != nil {
		p.lg.Info("stopping remote peer", zap.String("remote-peer-id", p.id.String()))
	}

	defer func() {
		if p.lg != nil {
			p.lg.Info("stopped remote peer", zap.String("remote-peer-id", p.id.String()))
		}
	}()

	close(p.stopc)
	p.cancel()
	p.msgAppV2Writer.stop()
	p.writer.stop()
	p.pipeline.stop()
	p.snapSender.stop()
	p.msgAppV2Reader.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
// pick 根据消息的类型选择合适的消息通道,并返回相应的通道供send()方法写入待发送的消息
func (p *peer) pick(m raftpb.Message) (writec chan<- raftpb.Message, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.
	// 如果是MsgSnap类型的消息,则返回Pipeline消息通道对应的Channel,否则返回Stream消息通道对应的Channel,
	// 如果Stream消息通道不可用,则使用Pipeline消息通道发送所有类型的消息
	if isMsgSnap(m) {
		return p.pipeline.msgc, pipelineMsg
	} else if writec, ok = p.msgAppV2Writer.writec(); ok && isMsgApp(m) {
		return writec, streamAppV2
	} else if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg
}

func isMsgApp(m raftpb.Message) bool { return m.Type == raftpb.MsgApp }

func isMsgSnap(m raftpb.Message) bool { return m.Type == raftpb.MsgSnap }
