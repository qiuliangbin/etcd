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

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"

	"go.uber.org/zap"
)

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

// A key-value stream backed by raft
type raftNode struct {
	// HTTP PUT请求表示添加键值对数据,当收到HTTP PUT请求时,
	// httpKVAPI会将请求中的键值信息通过proposeC通道传递给raftNode实例处理
	proposeC <-chan string `json:"propose_c,omitempty"` // proposed messages (k,v)
	// HTTP POST请求表示集群节点修改的请求,当收到POST请求时,httpKVAPI会通过confChangeC
	// 通道将修改的节点ID传递给raftNode实例进行处理
	confChangeC <-chan raftpb.ConfChange `json:"conf_change_c,omitempty"` // proposed cluster config changes
	// 在创建raftNode实例之后(raftNode实例的创建过程是在newRaftNode()函数中完成的)会
	// 返回commitC、errorC、snapshotterReady三个通道.raftNode会将etcd-raft模块
	// 返回的待应用Entry记录(封装在Ready实例中)写入commitC通道.另一方面,kvstore会从
	// commitC通道中读取这些待应用的Entry记录并保存其中的键值对信息.
	commitC chan<- *commit `json:"commit_c,omitempty"` // entries committed to log (k,v)
	// 当etcd-raft模块关闭或是出现异常的时候,会通过errorC通道将该信息通知上层模块
	errorC chan<- error `json:"error_c,omitempty"` // errors from raft session
	// 记录当前节点的ID
	id int `json:"id,omitempty"` // client ID for raft session
	// 当前集群中所有节点的地址, 当前节点会通过该字段中保存的地址向集群中其他节点发送消息.
	peers []string `json:"peers,omitempty"` // raft peer URLs
	// 当前节点是否为后续加入到一个集群的节点
	join bool `json:"join,omitempty"` // node is joining an existing cluster
	// 存放WAL日志文件的目录
	waldir string `json:"waldir,omitempty"` // path to WAL directory
	// 存放快照文件的目录
	snapdir string `json:"snapdir,omitempty"` // path to snapshot directory
	// 用于获取快照数据的函数,函数中调用kvstore.getSnapshot()方法获取kvstore.kvStore字段的数据
	getSnapshot func() ([]byte, error) `json:"get_snapshot,omitempty"`
	// 用于记录当前的集群状态,该状态就是从node.confstatec通道中获取的.
	confState raftpb.ConfState `json:"conf_state"`
	// 保存当前快照的相关元数据,即快照所包含的最后一条Entry记录的索引值
	snapshotIndex uint64 `json:"snapshot_index,omitempty"`
	appliedIndex  uint64 `json:"applied_index,omitempty"`

	// raft backing for the commit/error channel
	// etcd-raft模块中的node实例,node实现了Node接口,并将etcd-raft模块的API接口暴露给了上层模块
	node raft.Node `json:"node,omitempty"`
	// 参照 Storage接口及其具体实现MemoryStorage,在raftexample示例中,
	// 该MemoryStorage实例与底层raftLog.storage字段指向了同一个实例
	raftStorage *raft.MemoryStorage `json:"raft_storage,omitempty"`
	// 负责WAL日志的管理. 当节点收到一条Entry记录时,首先会将其保存到raftLog.unstable中,
	// 之后会将其封装到Ready实例中并交给上层模块发送给集群中的其他节点,并完成持久化.
	wal *wal.WAL `json:"wal,omitempty"`
	// 负责管理快照数据, etcd-raft模块并没有完成快照数据的管理,而是将其独立成一个单独的模块,
	snapshotter *snap.Snapshotter `json:"snapshotter,omitempty"`
	// 该通道用于通知上层模块snapshotter实例是否已经创建完成.
	snapshotterReady chan *snap.Snapshotter `json:"snapshotter_ready,omitempty"` // signals when snapshotter is ready
	// 两次生成快照之间间隔的Entry记录数,即当前节点每处理一定数量的Entry记录,就要触发一次快照数据的创建。
	// 每次生成快照时,即可是否掉一定数量的WAL日志及raftLog中保存的Entry记录,
	// 从而避免大量Entry记录带来的内存压力及大量的WAL日志文件袋里的磁盘压力;
	// 另外,定期创建快照也能减少节点重启时回放的WAL日志数量,加速启动时间
	snapCount uint64 `json:"snap_count,omitempty"`
	// 节点待发送的消息只是记录到了raft.msgs中,etcd-raft模块并没有提供网络层的实现,而由上层模块决定两个节点之间如何通信.
	transport *rafthttp.Transport `json:"transport,omitempty"`
	// 关闭proposeC通道
	stopc chan struct{} `json:"stopc,omitempty"` // signals proposal channel closed
	// 下面两通道协同工作,完成当前节点的关闭
	httpstopc chan struct{} `json:"httpstopc,omitempty"` // signals http server to shutdown
	httpdonec chan struct{} `json:"httpdonec,omitempty"` // signals http server shutdown complete
	// 日志
	logger *zap.Logger `json:"logger,omitempty"`
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed(重新播放) over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
// newRaftNode 启动raft实例,并发挥提交的日志条目通道和错误通道。
// 日志更新消息通过proposeC通道发送；
// 所有的日志条目通过commitC通道进行重放。(这里的重放的消息指的是通过proposeC通道发送到raftNode的消息)，
// 紧接着是一条nil消息(表示通道是当前的），然后是新的日志条目.
// 当关闭RaftNode时, 一定要关闭proposeC和读取errorC通道消息
func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *commit, <-chan error, <-chan *snap.Snapshotter) {
	// channel主要传输Entry记录
	// raftNode会将etcd-raft模块返回的待应用Entry记录(封装成Ready对象)写入commitC通道,
	// 然后,kvstore会从commitC通道中读取待应用的Entry记录,并保存其中的键值对信息.
	commitC := make(chan *commit)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		// 初始化存放WAL日志和SnapShot文件的目录
		waldir:      fmt.Sprintf("raftexample-%d", id),
		snapdir:     fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		// 创建stopc、httpstopc、httpdonec和snapshotterReady四个通道
		stopc:     make(chan struct{}),
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),

		logger: zap.NewExample(),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
		// 其余字段在WAL日志回放完成之后才会初始化
	}
	// 单独启动一个goroutine执行startRaft()方法,在该方法中完成剩余初始化操作
	go rc.startRaft()
	// 将commitC、errorC、snapshotterReady三个通道返回给上层应用
	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &commit{data, applyDoneC}:
		case <-rc.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(rc.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(rc.logger, rc.waldir)
		if err != nil {
			log.Fatalf("raftexample: error listing snapshots (%v)", err)
		}
		snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatalf("raftexample: error loading snapshot (%v)", err)
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) { // 检测WAL日志目录是否存在,如果不存在则进行创建
		if err := os.Mkdir(rc.waldir, 0750); err != nil { // 异常容错处理
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}
		// 新建WAL实例, 其中会创建相应目录和一个空的WAL日志文件
		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close() // 关闭WAL,其中包括各种关闭目录、文件和相关的goroutine
	}
	// 创建walsnap.Snapshot实例并初始化其Index字段和Term字段, 注意两者的区别:walsnap.Snapshot只
	// 包含了快照元数据中的Term值和索引值,并不会包含真正的快照数据
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap) // 创建WAL实例,异常处理
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	// 1.读取快照文件
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)       //根据读取到的Snapshot实例的元数据创建WAL实例
	_, st, ents, err := w.ReadAll() // 读取快照数据之后的全部WAL日志数据,并获取状态信息
	if err != nil {                 // 异常检测, 若读取WAL日志文件的过程中出现异常,则输出日志并终止程序
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	// 2.创建MemoryStorage实例, MemoryStorage的作用是"stable storage",维护快照数据及快照之后的所有Entry记录
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	// 3.将读取WAL日志之后得到的HardState加载到MemoryStorage中
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	// 4.将读取WAL日志得到的Entry记录加载到MemoryStorage中
	rc.raftStorage.Append(ents)

	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	// 检测snapdir字段指定的目录是否存在, 该目录用于存放定期生成的快照数据;
	// 若snapdir目录不存在,则进行创建; 若创建失败,则输出异常日志并终止程序
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	// 1.创建Snapshotter实例,并将该Snapshotter实例返回给上层模块
	// 注:应用Snapshotter实例提供了读写快照文件的功能
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	// 2.创建WAL实例, 然后加载快照并回放WAL日志
	oldwal := wal.Exist(rc.waldir) //检测waldir目录下是否存在旧的WAL日志文件
	rc.wal = rc.replayWAL()        // 在replayWAL()方法中会先加载快照数据,然后重放WAL日志文件

	// signal replay has finished
	rc.snapshotterReady <- rc.snapshotter //通知上层模块snapshotter已经创建完成
	// 3.创建raft.Config实例. 其中包含了启动etcd-raft模块的所有配置.
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,             // 选举超时时间
		HeartbeatTick:             1,              // 心跳超时时间
		Storage:                   rc.raftStorage, // 持久化存储,与etcd-raft模块中的raftLog.storage共享同一个MemoryStorage实例
		MaxSizePerMsg:             1024 * 1024,    // 每条消息的最大长度
		MaxInflightMsgs:           256,            // 已发送但是未响应的消息上限个数
		MaxUncommittedEntriesSize: 1 << 30,        // 未提交的消息上限个数
	}
	// 4.初始化底层的etcd-raft模块,这里会根据WAL的日志回放情况,判断当前节点是首次启动还是重新启动
	if oldwal || rc.join { // 如果存在旧的WAL日志文件,重新启动
		rc.node = raft.RestartNode(c)
	} else { // 当前节点首次启动
		rc.node = raft.StartNode(c, rpeers)
	}
	// 5.创建Transport实例并启动,它负责raft节点之间通信的网络服务
	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}
	// 启动网络服务相关组件
	rc.transport.Start()
	// 6.建立与集群中其他各个节点的链接
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}
	// 7.启动一个goroutine,其中会监听当前节点与集群中其他节点之间的网络连接
	go rc.serveRaft()
	// 8.启动后台goroutine处理上层应用于底层etcd-raft模块的交互
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)
	// 当前快照索引ID应当大于已应用索引ID
	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	// 触发kvstore去加载snapshot
	rc.commitC <- nil // trigger kvstore to load snapshot
	// 当前快照配置状态
	rc.confState = snapshotToSave.Metadata.ConfState
	// 当前快照索引
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	// 当前快照任期
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

// serveRaft 负责上层应用与底层etcd-raft模块的交互,主要处理proposeC和confChangeC通道的Entry消息
func (rc *raftNode) serveChannels() {
	// 获取快照实例,包含快照待应用记录、快照数据、和快照元数据
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()
	// 创建一个每隔100ms触发一次的定时器,那么在逻辑上,100ms即是etcd-raft组件的最小时间单位,
	// 该定时器每触发一次,则逻辑时钟就推进一次
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	// 1.单独启动一个goroutine负责将proposeC和confChangeC通道上接收的Entry消息
	// 传递给etcd-raft组件进行处理
	go func() {
		confChangeCount := uint64(0)
		// 循环从proposeC和confChangeC通道读取Entry消息
		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					// 读取proposeC异常,将proposeC通道置空
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					// 阻塞直到消息被(底层etcd-raft模块)处理
					rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					// 读取confChangeC异常,将confChangeC通道置空
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		// 关闭stopc通道, 触发raftNode.stop()方法的调用
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	// 循环处理etcd-raft模块返回给上层模块的数据及其它相关操作(etcd-raft状态机上的更新操作)
	for {
		select {
		case <-ticker.C:
			// ticker定时器超时触发一次
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		// 将raft条目存储到WAL,然后发布到commitC通道
		case rd := <-rc.node.Ready():
			// Must save the snapshot file and WAL snapshot entry before saving any other entries
			// or hardstate to ensure that recovery after a snapshot restore is possible.
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 将新的快照数据写入快照文件中
				rc.saveSnap(rd.Snapshot)
			}
			// 将当前etcd-raft组件的状态信息,以及待持久化的Entry记录先写入到WAL日志文件中
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 将新快照持久化到raftStorage
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				// 通知kvstore加载新快照
				// 将snapshot的消息通知给snapshot回放模块，etcd-raft的示例kv存储应用有一个后台协程会定期接收该消息，加载、回放最新的snapshot。
				// 在示例应用中，回放snapshot其实就是将snapshot中的数据反序列化为内存的kv。
				rc.publishSnapshot(rd.Snapshot)
			}
			// 将待持久化的Entry记录追加到raftStorage中完成持久化
			rc.raftStorage.Append(rd.Entries)
			/// 广播发送日志到所有节点（提交。提交成功的原则：多数节点确认提交）
			rc.transport.Send(rc.processMessages(rd.Messages))
			// 将已提交、待应用的 Entry 记录应用到所有节点的状态机中
			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}
			// 随着节点的运行， WAL 日志量和 raftLog.storage 中的 Entry 记录会不断增加 ，
			// 所以节点每处理 10000 条(默认值) Entry 记录，就会触发一次创建快照的过程，
			// 同时 WAL 会释放一些日志文件的句柄，raftLog.storage 也会压缩其保存的 Entry 记录
			rc.maybeTriggerSnapshot(applyDoneC)
			// 上层应用处理完该 Ready 实例，通知 etcd-raft 准备返回下一个 Ready 实例
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (rc *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = rc.confState
		}
	}
	return ms
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
