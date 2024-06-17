package replica

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

func (r *Replica) Step(m Message) error {

	switch {
	case m.Term == 0: // 本地消息
		// r.Warn("term is zero", zap.Uint64("nodeID", r.nodeID), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To))
	case m.Term > r.replicaLog.term:
		r.Debug("received message with higher term", zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.String("msgType", m.MsgType.String()))
		// 高任期消息
		if m.MsgType == MsgPing || m.MsgType == MsgLeaderTermStartIndexResp || m.MsgType == MsgSyncResp {
			if r.role == RoleLearner {
				r.becomeLearner(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, m.From)
			}

		} else {
			if r.role == RoleLearner {
				r.Warn("become learner but leader is none", zap.Uint64("nodeId", r.nodeId), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.String("msgType", m.MsgType.String()))
				r.becomeLearner(m.Term, None)
			} else {
				r.Warn("become follower but leader is none", zap.Uint64("nodeId", r.nodeId), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.String("msgType", m.MsgType.String()))
				r.becomeFollower(m.Term, None)
			}

		}
	case m.Term < r.replicaLog.term:
		if m.MsgType != MsgLeaderTermStartIndexResp {
			r.Warn("received message with lower term", zap.Uint32("term", m.Term), zap.Uint32("currentTerm", r.replicaLog.term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.String("msgType", m.MsgType.String()))
			if m.MsgType == MsgLeaderTermStartIndexReq {
				return r.stepFunc(m)
			}
			return nil
		}
	default:

	}

	switch m.MsgType {
	case MsgStoreAppendResp: // 存储追加响应
		if m.Index != 0 {
			// r.Debug("stableTo", zap.Uint64("index", m.Index), zap.Uint64("nodeId", r.nodeId), zap.Uint64("from", m.From), zap.Uint64("to", m.To))
			r.replicaLog.stableTo(m.Index)
		}
	case MsgApplyLogsResp: // 应用日志响应
		if m.CommittedIndex > r.replicaLog.appliedIndex {
			var size logEncodingSize = 0
			r.replicaLog.appliedTo(m.CommittedIndex, size)
			r.reduceUncommittedSize(size)
		}

	case MsgHup:
		r.hup()
	case MsgVoteReq: // 收到投票请求
		if r.canVote(m) {
			r.send(r.newMsgVoteResp(m.From, m.Term, false))
			r.electionElapsed = 0
			r.voteFor = m.From
			r.Info("agree vote", zap.Uint64("voteFor", m.From), zap.Uint32("term", m.Term), zap.Uint64("index", m.Index))
		} else {
			if r.voteFor != None {
				r.Info("already vote for other", zap.Uint64("voteFor", r.voteFor))
			} else if m.Index < r.replicaLog.lastLogIndex {
				r.Info("lower config version, reject vote")
			} else if m.Term < r.replicaLog.term {
				r.Info("lower term, reject vote")
			}
			r.send(r.newMsgVoteResp(m.From, m.Term, true))
		}
	case MsgConfigResp:
		confData := m.Logs[0].Data
		if len(confData) > 0 {
			cfg := NewConfig()
			err := cfg.Unmarshal(confData)
			if err != nil {
				r.Error("unmarshal config failed", zap.Error(err))
				return err
			}
			// 发送配置改变消息
			r.send(r.newMsgConfigChange(confData))
		}

	default:
		err := r.stepFunc(m)
		if err != nil {
			return err
		}
	}

	return nil
}

// 是否可以投票
func (r *Replica) canVote(m Message) bool {
	return (r.voteFor == None || (r.voteFor == m.From && r.leader == None)) && m.Index >= r.replicaLog.lastLogIndex && m.Term >= r.replicaLog.term
}

func (r *Replica) stepLeader(m Message) error {

	switch m.MsgType {
	case MsgPong:
		if m.To != r.nodeId {
			r.Warn("receive pong, but msg to is not self", zap.Uint64("nodeId", r.nodeId), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To))
			return nil
		}
		if m.Term != r.replicaLog.term {
			r.Warn("receive pong, but msg term is not self term", zap.Uint64("nodeId", r.nodeId), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To))
			return nil
		}
		// r.Debug("receive pong", zap.Uint64("nodeID", r.nodeID), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.Uint64("leaderCommittedIndex", r.replicaLog.committedIndex), zap.Uint64("committedIndex", m.CommittedIndex))
		r.activeReplica(m.From) // 副本已激活
	case MsgPropose: // 收到提案消息
		if len(m.Logs) == 0 {
			r.Panic("MsgPropose logs is empty", zap.Uint64("nodeId", r.nodeId))
		}
		if !r.appendLog(m.Logs...) {
			return ErrProposalDropped
		}
		if r.IsSingleNode() || r.opts.AckMode == AckModeNone { // 单机
			r.Debug("no ack", zap.Uint64("nodeId", r.nodeId), zap.Uint32("term", r.replicaLog.term), zap.Uint64("lastLogIndex", r.replicaLog.lastLogIndex), zap.Uint64("committedIndex", r.replicaLog.committedIndex))
			r.updateLeaderCommittedIndex() // 更新领导的提交索引
		}

	case MsgBeat:
		r.sendPing()
	case MsgSyncReq: // 追随者向领导同步消息
		// r.Info("recv sync", zap.Uint64("nodeID", r.nodeID), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.Uint64("index", m.Index), zap.Uint64("committedIndex", m.CommittedIndex))
		r.activeReplica(m.From)
		var needSendResp bool = true
		lastIndex := r.replicaLog.lastIndex()
		if lastIndex > 0 {
			if m.Index <= lastIndex {
				index := m.Index
				needSendResp = false // 发送MsgSyncGet后不需要发送MsgSyncResp了 交给上层异步获取日志后发送MsgSyncResp
				unstableLogs, err := r.replicaLog.getLogsFromUnstable(index, lastIndex+1, logEncodingSize(r.opts.SyncLimitSize))
				if err != nil {
					r.Error("get logs from unstable failed", zap.Error(err))
					return err
				}
				// fmt.Println("index", index, "lastIndex", lastIndex, "unstableLogs", len(unstableLogs))

				r.send(r.newMsgSyncGet(m.From, index, unstableLogs))

				// if len(logs) > 0 {
				// 	r.Debug("recv sync", zap.Uint64("nodeID", r.nodeID), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.Uint64("index", m.Index), zap.Uint64("firstLogIndex", logs[0].Index), zap.Uint64("lastLogIndex", logs[len(logs)-1].Index), zap.Uint64("localLastLogIndex", r.replicaLog.lastIndex()), zap.Int("logCount", len(logs)))
				// }
				if !r.isLearner(m.From) {
					r.updateReplicSyncInfo(m)      // 更新副本同步信息
					r.updateLeaderCommittedIndex() // 更新领导的提交索引
				}

			} else {
				if !r.isLearner(m.From) {
					r.updateReplicSyncInfo(m)      // 更新副本同步信息
					r.updateLeaderCommittedIndex() // 更新领导的提交索引
				}
			}
		}
		if needSendResp {
			r.send(r.newMsgSyncResp(m.From, m.Index, nil))
		}

		if r.opts.AutoLearnerToFollower && r.isLearner(m.From) {

			// 如果迁移的源节点是领导者，那么学习者必须完全追上领导者的日志
			if r.opts.Config.MigrateFrom != 0 && r.opts.Config.MigrateFrom == r.leader {
				fmt.Println("migrate from leader--->", m.Index)
				if m.Index >= r.replicaLog.lastLogIndex+1 {
					r.send(r.newMsgLearnerToLeader(m.From))
				}
			} else {
				fmt.Println("migrate from follower--->", m.Index)
				// 如果learner的日志已经追上了follower的日志，那么将learner转为follower
				if m.Index+r.opts.LearnerToFollowerMinLogGap > r.replicaLog.lastLogIndex {
					// 发送配置改变消息
					r.send(r.newMsgLearnerToFollower(m.From))
				}
			}

		}

	case MsgSyncGetResp: // 领导已查询到追随者的日志了，发给追随者
		r.send(r.newMsgSyncResp(m.To, m.Index, m.Logs))

	case MsgLeaderTermStartIndexReq: // 领导收到跟随者的任期开始索引请求
		// 如果MsgLeaderTermStartIndexReq的term等于领导的term则领导返回当前最新日志下标，否则返回MsgLeaderTermStartIndexReq里的term+1的 任期的第一条日志下标，返回的这个值称为LastOffset
		if m.Term == r.replicaLog.term {
			r.send(r.newLeaderTermStartIndexResp(m.From, r.replicaLog.term, r.replicaLog.lastLogIndex+1)) // 当前最新日志下标 + 1 副本truncate日志到这个下标,也就是不会truncate
		} else {
			syncTerm := m.Term + 1
			lastIndex, err := r.opts.Storage.LeaderTermStartIndex(syncTerm)
			if err != nil {
				return err
			}
			if lastIndex == 0 {
				r.Error("leader term start index not found", zap.Uint32("term", syncTerm), zap.Uint32("leaderTerm", r.replicaLog.term))
				// return ErrLeaderTermStartIndexNotFound
			}
			r.Info("send leader term start index resp", zap.Uint64("nodeId", r.nodeId), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.Uint32("syncTerm", syncTerm), zap.Uint64("lastIndex", lastIndex))
			r.send(r.newLeaderTermStartIndexResp(m.From, syncTerm, lastIndex)) // 副本truncate日志到这个下标（不会保留lastIndex的日志）
		}
	case MsgConfigReq: // 收到配置请求
		r.send(r.newMsgConfigResp(m.From))
	}

	return nil
}

func (r *Replica) stepFollower(m Message) error {
	switch m.MsgType {
	case MsgPing:
		r.electionElapsed = 0
		if r.leader == None {
			r.becomeFollower(m.Term, m.From)

		}
		if m.ConfVersion > r.opts.Config.Version { // 如果本地配置版本小于领导的配置版本，那么请求领导的配置
			r.send(r.newMsgConfigReq(m.From))
		}

		r.SetSpeedLevel(m.SpeedLevel) // 设置同步速度
		r.send(r.newPong(m.From))
		// r.Debug("recv ping", zap.Uint64("nodeID", r.nodeID), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.Uint64("lastLogIndex", r.replicaLog.lastLogIndex), zap.Uint64("leaderCommittedIndex", m.CommittedIndex), zap.Uint64("committedIndex", r.replicaLog.committedIndex))
		r.updateFollowCommittedIndex(m.CommittedIndex) // 更新提交索引
		r.replicaLog.leaderLastLogIndex = m.Index
	case MsgSyncResp: // 领导返回同步消息的结果
		r.SetSpeedLevel(m.SpeedLevel) // 设置同步速度
		r.electionElapsed = 0
		if len(m.Logs) > 0 {
			r.messageWait.immediatelySync()
			// r.Info("recv sync resp", zap.Int("logCount", len(m.Logs)), zap.Uint64("startLogIndex", m.Logs[0].Index), zap.Uint64("endLogIndex", m.Logs[len(m.Logs)-1].Index), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.Uint64("index", m.Index), zap.Uint64("committedIndex", m.CommittedIndex))
		} else {
			r.messageWait.quickSync()
		}
		if r.disabledToSync {
			r.Debug("disabled to sync", zap.Uint64("leader", r.leader), zap.Uint32("term", r.replicaLog.term))
			return nil
		}
		if len(m.Logs) > 0 {
			if m.Logs[len(m.Logs)-1].Index <= r.LastLogIndex() {
				r.Panic("append log reject", zap.Uint64("leader", r.leader), zap.Uint64("maxLogIndex", m.Logs[len(m.Logs)-1].Index), zap.Uint64("localLastLogIndex", r.LastLogIndex()))
				return nil
			}
			if !r.appendLog(m.Logs...) {
				return ErrProposalDropped
			}
		}
		r.replicaLog.leaderCommittedIndex = m.CommittedIndex
		// r.hasFirstSyncResp = true
		r.updateFollowCommittedIndex(m.CommittedIndex) // 更新提交索引

	case MsgLeaderTermStartIndexResp: // 收到领导的任期开始索引响应
		err := r.handleLeaderTermStartIndexResp(m.Index, m.Term)
		if err != nil {
			return err
		}

		r.disabledToSync = false // 现在可以去同步领导的日志了
		r.messageWait.resetSync()
		r.send(r.newSyncMsg()) // 立马发送同步消息
		r.Info("enable to sync", zap.Uint64("leader", r.leader), zap.Uint64("lastIndex", r.replicaLog.lastLogIndex), zap.Uint32("term", m.Term))
	}
	return nil
}

func (r *Replica) stepLearner(m Message) error {
	switch m.MsgType {
	case MsgPing:
		if r.leader == None {
			r.becomeLearner(m.Term, m.From)
		}
		if m.ConfVersion > r.opts.Config.Version { // 如果本地配置版本小于领导的配置版本，那么请求领导的配置

			r.send(r.newMsgConfigReq(m.From))
		}
		r.SetSpeedLevel(m.SpeedLevel) // 设置同步速度
		r.send(r.newPong(m.From))
		r.replicaLog.leaderLastLogIndex = m.Index
	case MsgSyncResp: // 领导返回同步消息的结果
		r.SetSpeedLevel(m.SpeedLevel) // 设置同步速度
		r.electionElapsed = 0
		r.messageWait.quickSync()
		if r.disabledToSync {
			r.Debug("learner: disabled to sync", zap.Uint64("leader", r.leader), zap.Uint32("term", r.replicaLog.term))
			return nil
		}
		if len(m.Logs) > 0 {
			if m.Logs[len(m.Logs)-1].Index <= r.LastLogIndex() {
				r.Panic("learner: append log reject", zap.Uint64("leader", r.leader), zap.Uint64("maxLogIndex", m.Logs[len(m.Logs)-1].Index), zap.Uint64("localLastLogIndex", r.LastLogIndex()))
				return nil
			}
			if !r.appendLog(m.Logs...) {
				return ErrProposalDropped
			}
		}
		r.replicaLog.leaderCommittedIndex = m.CommittedIndex
	case MsgLeaderTermStartIndexResp: // 收到领导的任期开始索引响应
		err := r.handleLeaderTermStartIndexResp(m.Index, m.Term)
		if err != nil {
			return err
		}
		r.disabledToSync = false        // 现在可以去同步领导的日志了
		r.messageWait.immediatelySync() // 立马可以同步了
		r.Info("learner: enable to sync", zap.Uint64("leader", r.leader), zap.Uint64("lastIndex", r.replicaLog.lastLogIndex), zap.Uint32("term", m.Term))
	}
	return nil
}

func (r *Replica) stepCandidate(m Message) error {
	switch m.MsgType {
	case MsgPing:
		if m.ConfVersion > r.opts.Config.Version { // 如果本地配置版本小于领导的配置版本，那么请求领导的配置
			r.send(r.newMsgConfigReq(m.From))
		}
		r.becomeFollower(m.Term, m.From)
		r.send(r.newPong(m.From))
	case MsgVoteResp:
		r.Info("received vote response", zap.Bool("reject", m.Reject), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.Uint32("term", m.Term), zap.Uint64("index", m.Index))
		r.poll(m)
	}
	return nil
}

func (r *Replica) poll(m Message) {
	r.votes[m.From] = !m.Reject
	var granted int
	for _, v := range r.votes {
		if v {
			granted++
		}
	}
	if len(r.votes) < r.quorum() { // 投票数小于法定数
		return
	}
	if granted >= r.quorum() {
		r.becomeLeader(r.replicaLog.term) // 成为领导者
		r.sendPing()
	} else {
		r.becomeFollower(r.replicaLog.term, None)
	}
}

func (r *Replica) appendLog(logs ...Log) (accepted bool) {
	if len(logs) == 0 {
		return true
	}

	if !r.increaseUncommittedSize(logs) {
		r.Warn("appending new logs would exceed uncommitted log size limit; dropping proposal", zap.Uint64("size", uint64(r.uncommittedSize)), zap.Uint64("max", r.opts.MaxUncommittedLogSize))
		return false
	}
	if logs[0].Index != r.LastLogIndex()+1 { // 连续性判断
		r.Panic("log index is not continuous", zap.Uint64("lastLogIndex", r.replicaLog.lastLogIndex), zap.Uint64("startLogIndex", logs[0].Index), zap.Uint64("endLogIndex", logs[len(logs)-1].Index))
		return false
	}

	if after := logs[0].Index; after < r.replicaLog.committedIndex {
		r.Panic("log index is out of range", zap.Uint64("after", after), zap.Int("logCount", len(logs)), zap.Uint64("lastIndex", r.replicaLog.lastIndex()), zap.Uint64("committed", r.replicaLog.committedIndex))
		return false
	}

	for _, lg := range logs {
		if r.localLeaderLastTerm != lg.Term {
			r.localLeaderLastTerm = lg.Term
			err := r.opts.Storage.SetLeaderTermStartIndex(lg.Term, lg.Index)
			if err != nil {
				r.Panic("set leader term start index failed", zap.Error(err))
				return false
			}
		}
	}
	if len(logs) == 0 {
		return true
	}
	r.replicaLog.appendLog(logs...)
	return true
}

func (r *Replica) handleLeaderTermStartIndexResp(index uint64, term uint32) error {
	// Follower检查本地的LeaderTermSequence
	// 是否有term对应的StartOffset大于领导返回的LastOffset，
	// 如果有则将当前term的startOffset设置为LastOffset，
	// 并且当前term为最新的term（也就是删除比当前term大的LeaderTermSequence的记录）
	if index > 0 {
		termStartIndex, err := r.opts.Storage.LeaderTermStartIndex(term)
		if err != nil {
			r.Error("follower: leader term start index not found", zap.Uint32("term", term))
			return err
		}
		if termStartIndex == 0 {
			err := r.opts.Storage.SetLeaderTermStartIndex(term, index)
			if err != nil {
				r.Error("set leader term start index failed", zap.Error(err))
				return err
			}
		} else if termStartIndex > index {
			err := r.opts.Storage.SetLeaderTermStartIndex(term, index)
			if err != nil {
				r.Error("set leader term start index failed", zap.Error(err))
				return err
			}
			err = r.opts.Storage.DeleteLeaderTermStartIndexGreaterThanTerm(term)
			if err != nil {
				r.Error("delete leader term start index failed", zap.Error(err))
				return err
			}
		}
		r.Info("truncate log to", zap.Uint64("leader", r.leader), zap.Uint32("term", term), zap.Uint64("index", index))

		err = r.opts.Storage.TruncateLogTo(index)
		if err != nil {
			r.Error("truncate log failed", zap.Error(err))
			return err
		}
		r.replicaLog.unstable.truncateLogTo(index)
		lastIdx := r.replicaLog.lastIndex()
		r.replicaLog.lastLogIndex = lastIdx
		r.localLeaderLastTerm = term
	}
	return nil
}

// func (r *Replica) getLogs(startLogIndex uint64, endLogIndex uint64, limit uint32) ([]Log, error) {
// 	if r.unstable.len() > 0 && r.unstable.FirstIndex() <= startLogIndex {
// 		logs, err := r.unstable.Logs(startLogIndex, endLogIndex, limit)
// 		if err != nil {
// 			return nil, err
// 		}
// 		return logs, nil
// 	}

// 	logs, err := r.opts.Storage.Logs(startLogIndex, endLogIndex, limit)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return logs, nil

// }

func (r *Replica) increaseUncommittedSize(logs []Log) bool {
	var size logEncodingSize
	for _, l := range logs {
		size += logEncodingSize(l.LogSize())
	}
	if r.uncommittedSize > 0 && size > 0 && r.uncommittedSize+size > logEncodingSize(r.opts.MaxUncommittedLogSize) {
		return false
	}
	r.uncommittedSize += size
	return true
}

func (r *Replica) send(m Message) {
	r.msgs = append(r.msgs, m)
	// if m.MsgType == MsgSyncResp {
	// 	index, ok := r.testMap[m.To]
	// 	if ok {
	// 		if m.Index == index {
	// 			r.Panic("send same sync resp", zap.Uint64("nodeID", r.nodeID), zap.Uint32("term", m.Term), zap.Uint64("from", m.From), zap.Uint64("to", m.To), zap.Uint64("index", m.Index))
	// 		}
	// 	}
	// 	r.testMap[m.To] = m.Index
	// }

}

// // 广播通知同步消息
// func (r *Replica) bcastNotifySync() {
// 	for _, replicaID := range r.replicas {
// 		if replicaID == r.nodeID {
// 			continue
// 		}
// 		r.send(r.newNotifySyncMsg(replicaID))
// 	}
// }

// 获取跟随者的提交索引
func (r *Replica) committedIndexForFollow(leaderCommittedIndex uint64) uint64 {
	if leaderCommittedIndex > r.replicaLog.committedIndex {
		return min(leaderCommittedIndex, r.replicaLog.lastLogIndex)

	}
	return r.replicaLog.committedIndex
}

func (r *Replica) quorum() int {
	return (len(r.replicas)+1)/2 + 1 //  r.replicas 不包含本节点
}

// 通过副本同步信息计算已提交下标
func (r *Replica) committedIndexForLeader() uint64 {

	committed := r.replicaLog.committedIndex
	quorum := r.quorum() // r.replicas 不包含本节点
	if quorum <= 1 {     // 如果少于或等于一个节点，那么直接返回最后一条日志下标
		return r.replicaLog.lastIndex()
	}

	// 获取比指定参数小的最大日志下标
	getMaxLogIndexLessThanParam := func(maxIndex uint64) uint64 {
		secondMaxIndex := uint64(0)
		for _, syncInfo := range r.lastSyncInfoMap {
			if syncInfo.LastSyncLogIndex < maxIndex || maxIndex == 0 {
				if secondMaxIndex < syncInfo.LastSyncLogIndex {
					secondMaxIndex = syncInfo.LastSyncLogIndex
				}
			}
		}
		if secondMaxIndex > 0 {
			return secondMaxIndex - 1
		}
		return secondMaxIndex
	}

	maxLogIndex := uint64(0)
	newCommitted := uint64(0)
	for {
		count := 0
		maxLogIndex = getMaxLogIndexLessThanParam(maxLogIndex)
		if maxLogIndex == 0 {
			break
		}
		if maxLogIndex <= committed {
			break
		}
		if maxLogIndex > r.replicaLog.lastLogIndex {
			continue
		}
		for _, syncInfo := range r.lastSyncInfoMap {
			if syncInfo.LastSyncLogIndex-1 >= maxLogIndex {
				count++
			}
			if count+1 >= quorum {
				newCommitted = maxLogIndex
				break
			}
		}
	}
	if newCommitted > committed {
		return min(newCommitted, r.replicaLog.lastLogIndex)
	}
	return committed

}

// 更新跟随者的提交索引
func (r *Replica) updateFollowCommittedIndex(leaderCommittedIndex uint64) {
	if leaderCommittedIndex == 0 || leaderCommittedIndex <= r.replicaLog.committedIndex {
		return
	}
	newCommittedIndex := r.committedIndexForFollow(leaderCommittedIndex)
	if newCommittedIndex > r.replicaLog.committedIndex {
		r.replicaLog.committedIndex = newCommittedIndex
		// r.Debug("update follow committed index", zap.Uint64("nodeId", r.nodeId), zap.Uint32("term", r.replicaLog.term), zap.Uint64("committedIndex", r.replicaLog.committedIndex))
	}
}

// 更新领导的提交索引
func (r *Replica) updateLeaderCommittedIndex() bool {
	newCommitted := r.committedIndexForLeader() // 通过副本同步信息计算领导已提交下标
	updated := false
	if newCommitted > r.replicaLog.committedIndex {
		r.replicaLog.committedIndex = newCommitted
		updated = true
		// r.Debug("update leader committed index", zap.Uint64("lastIndex", r.LastLogIndex()), zap.Uint32("term", r.replicaLog.term), zap.Uint64("committedIndex", r.replicaLog.committedIndex))
	}
	return updated
}

// 更新副本同步信息
func (r *Replica) updateReplicSyncInfo(m Message) {
	from := m.From
	syncInfo := r.lastSyncInfoMap[from]
	if syncInfo == nil {
		syncInfo = &SyncInfo{}
		r.lastSyncInfoMap[from] = syncInfo
	}
	if m.Index > syncInfo.LastSyncLogIndex {
		syncInfo.LastSyncLogIndex = m.Index
		syncInfo.LastSyncTime = uint64(time.Now().UnixNano())
		// r.Debug("update replic sync info", zap.Uint32("term", r.replicaLog.term), zap.Uint64("from", from), zap.Uint64("lastSyncLogIndex", syncInfo.LastSyncLogIndex))
	}
}

func (r *Replica) sendPingIfNeed() bool {
	if !r.isLeader() {
		return false
	}

	if r.IsSingleNode() { // 单节点不需要发ping
		return false
	}

	if !r.messageWait.canPing() {
		return false
	}

	hasPing := false
	for _, replicaId := range r.replicas {
		if replicaId == r.opts.NodeId {
			continue
		}
		if !r.isActiveReplica(replicaId) {
			r.send(r.newPing(replicaId))
			hasPing = true
		}
	}
	if len(r.opts.Config.Learners) > 0 {
		for _, replicaId := range r.opts.Config.Learners {
			if replicaId == r.opts.NodeId {
				continue
			}
			if !r.isActiveReplica(replicaId) {
				r.send(r.newPing(replicaId))
				hasPing = true
			}
		}
	}
	r.messageWait.resetPing()
	return hasPing

}

func (r *Replica) sendPing() {
	if !r.isLeader() {
		return
	}
	for _, replicaId := range r.replicas {
		if replicaId == r.opts.NodeId {
			continue
		}
		r.send(r.newPing(replicaId))
	}
	if len(r.opts.Config.Learners) > 0 {
		for _, replicaId := range r.opts.Config.Learners {
			if replicaId == r.opts.NodeId {
				continue
			}
			r.send(r.newPing(replicaId))
		}
	}
}

func (r *Replica) hasNeedPing() bool {
	if !r.isLeader() {
		return false
	}

	if r.IsSingleNode() { // 单节点不需要发ping
		return false
	}

	if !r.messageWait.canPing() {
		return false
	}

	for _, replicaId := range r.replicas {
		if replicaId == r.opts.NodeId {
			continue
		}

		if !r.isActiveReplica(replicaId) {
			return true
		}
	}
	return false
}
