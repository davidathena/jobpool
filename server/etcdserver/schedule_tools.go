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
	"time"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/schedulepb"
)

func convertEvalFromRequest(r *etcdserverpb.ScheduleEvalAddRequest) *domain.Evaluation {
	if r == nil {
		return &domain.Evaluation{}
	}
	data := &domain.Evaluation{
		ID:          r.Id,
		Namespace:   r.Namespace,
		Priority:    int(r.Priority),
		Type:        r.Type,
		TriggeredBy: r.TriggeredBy,
		JobID:       r.JobId,
		PlanID:      r.PlanId,
		Status:      r.Status,
	}
	// 还原延迟语义：带 WaitUntil 的 eval 入队时会进入延迟堆而非立即 ready
	if r.WaitUntil != nil {
		data.WaitUntil = time.Unix(r.WaitUntil.Seconds, int64(r.WaitUntil.Nanos))
	}
	return data
}

func convertEvalFromEvalucation(r *schedulepb.Evaluation) *domain.Evaluation {
	if r == nil {
		return &domain.Evaluation{}
	}
	data := &domain.Evaluation{
		ID:                r.Id,
		Namespace:         r.Namespace,
		Priority:          int(r.Priority),
		Type:              r.Type,
		TriggeredBy:       r.TriggeredBy,
		JobID:             r.JobId,
		PlanID:            r.PlanId,
		Status:            r.Status,
		NodeID:            r.NodeId,
		StatusDescription: r.StatusDescription,
		NextEval:          r.NextEval,
		PreviousEval:      r.PreviousEval,
		BlockedEval:       r.BlockedEval,
	}
	// 恢复延迟语义：带 WaitUntil 的 eval 入队时会进入延迟堆而非立即 ready
	if r.WaitUntil != nil {
		data.WaitUntil = time.Unix(r.WaitUntil.Seconds, int64(r.WaitUntil.Nanos))
	}
	return data
}
