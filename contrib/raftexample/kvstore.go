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
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

// a key-value store backed by raft
// kvstore kvstore扮演了持久化存储和状态机的角色,etcd-raft模块通过Ready实例返回的待应用Entry记录
// 最终都会存储到kvstore中
type kvstore struct {
	// httpKVAPI处理HTTP PUT请求时,会调用kvstore.Propose()方法将用户请求的数据写入proposeC通道中,
	// 之后raftNode会从该通道中读取数据并进行处理
	proposeC chan<- string // channel for proposing updates
	mu       sync.RWMutex
	// 用来存储键值对的map, 其中存储的键值都是string类型的
	kvStore map[string]string // current committed key-value pairs
	// 负责读取快照文件
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	// raftNode会将待应用的Entry记录写入commitC通道中,另外,当需要加载快照数据时,raftNode会向commitC通道中写入nil作为信号.
	// 在kvstore初始化(newKVStore()函数)时,会启动一个后台goroutine来执行kvstore.readCommits()来读取commitC通道,
	// 然后将读取到的键值对写入kvStore中
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

func (s *kvstore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC { // 循环读取commitC通道
		if commit == nil { // 读取到nil时,则表示需要读取快照数据
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot() //通过snapshotter读取快照文件
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil { //加载快照数据
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data { // 将读取到的数据进行反序列化得到kv实例
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			s.mu.Lock() // 加锁
			s.kvStore[dataKv.Key] = dataKv.Val
			s.mu.Unlock() //解锁
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// recoverFromSnapshot 将快照数据反序列化得到一个map实例,然后直接替换当前的kvStore实例
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	// 快照数据反序列化
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()         // 加锁
	defer s.mu.Unlock() //解锁
	s.kvStore = store
	return nil
}
