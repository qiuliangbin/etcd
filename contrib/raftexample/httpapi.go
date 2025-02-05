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
	"io"
	"log"
	"net/http"
	"strconv"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	// kvstore实例在raftexample示例中扮演了持久化存储的角色;用于保存用户提交的键值对信息.
	store *kvstore
	// 当用户发送POST(或者DELETE)请求时,会被认为是发送了一个集群节点增加(或删除)的请求,
	// httpKVAPI会将该请求的信息写入confChangeC通道,然后raftNode实例会读取confChange通道并进行相应响应.
	confChangeC chan<- raftpb.ConfChange
}

// ServeHTTP net/http的请求包解析完成后,回调ServeHTTP函数
func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI  // 获取请求的URL作为key
	defer r.Body.Close() // 释放Body资源
	switch r.Method {
	case http.MethodPut: // PUT请求的处理
		v, err := io.ReadAll(r.Body) // 读取HTTP请求体
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		// 在 kvstore.Propose()方法中会对键位对进行序列化, 之后将结果写入proposeC通道,
		// 后续raftNode会读取其中数据进行处理
		h.store.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent) // 向用户返回相应的状态码
	case http.MethodGet: // GET请求表示从kvstore实例中读取指定的键值对数据
		if v, ok := h.store.Lookup(key); ok { // 直接从kvstore中读取指定的键值对数据,并返回给用户
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound) //返回404
		}
	case http.MethodPost: // POST请求表示向集群中新增指定的节点
		url, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		// 解析URL得到新增节点的id
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		// 创建ConfChange消息
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode, // ConfChangeAddNode表示新增节点
			NodeID:  nodeId,                   // 指定新增节点的ID
			Context: url,                      // 指定新增节点的URL
		}
		h.confChangeC <- cc // 将ConfChange实例写入confChangeC通道中
		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent) // 返回相应状态码
	case http.MethodDelete: // DELETE请求的处理
		// 解析key得到的待删除的节点id, 异常处理
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode, //ConfChangeRemoveNode表示删除指定节点
			NodeID: nodeId,                      // 指定待删除的节点id
		}
		h.confChangeC <- cc // 将ConfChange实例发送到confChangeC通道中

		// As above, optimistic that raft will apply the conf change
		// 与PUT请求的处理方法类似,向客户返回状态码为204的HTTP响应
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	srv := http.Server{ // 创建http.Server用于接收HTTP请求
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{ // 设置http.Server的Handler字段
			store:       kv,
			confChangeC: confChangeC,
		},
	}
	// 启动单独的goroutine来监听Addr指定的地址,当有HTTP请求时,http.Server会创建对应的goroutine,
	// 并调用httpKVAPI.ServeHTTP方法进行处理
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
