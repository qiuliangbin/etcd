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
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")

type pipeline struct {
	// pipeline对应的节点ID
	peerID types.ID
	// 关联rafthttp.Transporter实例
	tr *Transport
	// 每个节点可能提供了多个URL供其他节点访问,当其中一个访问失败时,我们应该可以尝试访问另一个。
	// 而urlPicker提供的主要功能就是在这些URL之间进行切换
	picker *urlPicker

	status *peerStatus
	// 底层的Raft实例
	raft   Raft
	errorc chan error
	// deprecate when we depercate v2 API
	followerStats *stats.FollowerStats
	// pipeline实例从该通道中获取获取待发送的消息
	msgc chan raftpb.Message
	// wait for the handling routines
	// 负责同步多个goroutine结束. 每个pipeline实例会启动多个后台goroutine(默认值是4个)
	// 来处理msgc通道中的消息, 在pipeline.stop()方法中必须等待这些goroutine都结束(通过wg.Wait()方法实现),
	// 才能真正关闭该pipeline实例.
	wg    sync.WaitGroup
	stopc chan struct{}
}

// start 启动pipeline消息通道中的消息处理
func (p *pipeline) start() {
	// 创建stopc通道用于关闭pipeline消息通道内的消息处理
	p.stopc = make(chan struct{})
	// 注意缓存,默认大小为64, 主要是为了防止瞬间网络延迟造成消息丢失
	p.msgc = make(chan raftpb.Message, pipelineBufSize)
	p.wg.Add(connPerPipeline) // 初始化sync.WaitGroup
	for i := 0; i < connPerPipeline; i++ {
		go p.handle() // 启动用于发送消息的goroutine(默认是4个)
	}

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"started HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	}
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"stopped HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	}
}

// handle 从msgc通道中读取待发送的Message消息,然后调用pipeline.post()方法将消息发送出去,
// 发送结束之后会调用底层Raft接口的相应方法报告发送结果。
func (p *pipeline) handle() {
	defer p.wg.Done() // handle()方法执行完成,也就是当前这个goroutine结束

	for {
		select {
		case m := <-p.msgc: // 获取待发送的MsgSnap类型的消息
			start := time.Now()
			err := p.post(pbutil.MustMarshal(&m)) // 将消息序列化, 然后创建HTTP请求并发送出去
			end := time.Now()

			if err != nil {
				// 标记当前节点不活跃(网络不可达)
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())

				if m.Type == raftpb.MsgApp && p.followerStats != nil {
					p.followerStats.Fail()
				}
				// 向底层的Raft状态机报告失败消息
				p.raft.ReportUnreachable(m.To)
				if isMsgSnap(m) {
					p.raft.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
				// 标记当前节点访问失败加1
				sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
				continue
			}
			// 标记当前节点为活跃状态
			p.status.activate()
			if m.Type == raftpb.MsgApp && p.followerStats != nil {
				p.followerStats.Succ(end.Sub(start))
			}

			if isMsgSnap(m) {
				// 向底层的Raft状态机报告发送成功的消息
				p.raft.ReportSnapshot(m.To, raft.SnapshotFinish)
			}
			sentBytes.WithLabelValues(types.ID(m.To).String()).Add(float64(m.Size()))
		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,
// error on any failure.
// post 真正完成消息发送的地方,其中会启动一个后台goroutine监听控制发送过程及获取发送结果
func (p *pipeline) post(data []byte) (err error) {
	u := p.picker.pick() // 获取对端暴露的URL地址
	// 创建HTTP POST请求
	req := createPostRequest(p.tr.Logger, u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, p.tr.ClusterID)
	// 主要用于通知下面的goroutine请求是否已经发送完成
	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	go func() { // 该goroutine主要用于监听请求是否需要取消
		select {
		case <-done:
			cancel()
		case <-p.stopc: // 如果在请求发送过程中,pipeline被关闭, 则取消该请求
			waitSchedule()
			cancel() // 取消请求
		}
	}()
	// 发送上述HTTP POST请求, 并获取到对应的响应
	resp, err := p.tr.pipelineRt.RoundTrip(req)
	// 通知上述goroutine, 请求已经发送完毕
	done <- struct{}{}
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	defer resp.Body.Close()
	// 去读HttpResponse.Body内容
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	// 检测响应的内容
	err = checkPostResponse(p.tr.Logger, resp, b, req, p.peerID)
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { runtime.Gosched() }
