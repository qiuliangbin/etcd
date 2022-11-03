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
	"fmt"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
)

// Status contains information about this Raft peer and its view of the system.
// The Progress is only populated on the leader.
type Status struct {
	BasicStatus
	Config   tracker.Config              // 节点配置信息和唱票信息
	Progress map[uint64]tracker.Progress // 记录集群中每个节点对应的Progress实例
}

// BasicStatus contains basic information about the Raft peer. It does not allocate.
type BasicStatus struct {
	ID uint64 // 当前节点的ID

	pb.HardState // 当前节点的任期号&当前节点在该任期的投票结果&当前节点的raftLog的已提交位置
	SoftState    // 当前集群的Leader节点ID & 当前节点的角色

	Applied uint64 // 已应用的Entry记录的最大索引值

	LeadTransferee uint64 // Leader节点迁移到Follower节点ID(LeadTransferee)
}

func getProgressCopy(r *raft) map[uint64]tracker.Progress {
	m := make(map[uint64]tracker.Progress)
	r.prs.Visit(func(id uint64, pr *tracker.Progress) {
		p := *pr
		p.Inflights = pr.Inflights.Clone()
		pr = nil

		m[id] = p
	})
	return m
}

func getBasicStatus(r *raft) BasicStatus {
	s := BasicStatus{
		ID:             r.id,
		LeadTransferee: r.leadTransferee,
	}
	s.HardState = r.hardState()
	s.SoftState = *r.softState()
	s.Applied = r.raftLog.applied
	return s
}

// getStatus gets a copy of the current raft status.
func getStatus(r *raft) Status {
	var s Status
	s.BasicStatus = getBasicStatus(r)
	if s.RaftState == StateLeader {
		s.Progress = getProgressCopy(r)
	}
	s.Config = r.prs.Config.Clone()
	return s
}

// MarshalJSON translates the raft status into JSON.
// TODO: try to simplify this by introducing ID type into raft
func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%q,"applied":%d,"progress":{`,
		s.ID, s.Term, s.Vote, s.Commit, s.Lead, s.RaftState, s.Applied)

	if len(s.Progress) == 0 {
		j += "},"
	} else {
		for k, v := range s.Progress {
			subj := fmt.Sprintf(`"%x":{"match":%d,"next":%d,"state":%q},`, k, v.Match, v.Next, v.State)
			j += subj
		}
		// remove the trailing ","
		j = j[:len(j)-1] + "},"
	}

	j += fmt.Sprintf(`"leadtransferee":"%x"}`, s.LeadTransferee)
	return []byte(j), nil
}

func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		getLogger().Panicf("unexpected error: %v", err)
	}
	return string(b)
}
