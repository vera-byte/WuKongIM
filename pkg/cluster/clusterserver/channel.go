package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/reactor"
	replica "github.com/WuKongIM/WuKongIM/pkg/cluster/replica2"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var _ reactor.IHandler = &channel{}

type channel struct {
	key         string
	channelId   string
	channelType uint8
	rc          *replica.Replica
	opts        *Options
	isPrepared  bool
	wklog.Log
	mu             sync.Mutex
	cfg            wkdb.ChannelClusterConfig
	pausePropopose atomic.Bool // 是否暂停提案

	sendConfigTick        int // 发送配置计数器
	sendConfigTimeoutTick int // 发送配置超时（达到这个tick表示，需要发送配置请求了）

	sendConfigReqToSlotLeader func(c *channel, cfgVersion uint64) error // 向槽领导发送配置请求

	s *Server
}

func newChannel(channelId string, channelType uint8, opts *Options, s *Server, sendConfigReqToSlotLeader func(c *channel, cfgVersion uint64) error) *channel {
	key := ChannelToKey(channelId, channelType)
	c := &channel{
		key:                       key,
		channelId:                 channelId,
		channelType:               channelType,
		sendConfigReqToSlotLeader: sendConfigReqToSlotLeader,
		sendConfigTimeoutTick:     10,
		opts:                      opts,
		Log:                       wklog.NewWKLog(fmt.Sprintf("cluster.channel[%s]", key)),
		s:                         s,
	}

	appliedIdx, err := c.opts.MessageLogStorage.AppliedIndex(c.key)
	if err != nil {
		c.Panic("get applied index error", zap.Error(err))

	}
	lastIndex, lastTerm, err := c.opts.MessageLogStorage.LastIndexAndTerm(c.key)
	if err != nil {
		c.Panic("get last index and term error", zap.Error(err))
	}
	rc := replica.New(
		c.opts.NodeId,
		replica.WithLogPrefix(fmt.Sprintf("channel-%s", c.key)),
		replica.WithAppliedIndex(appliedIdx),
		replica.WithElectionOn(false),
		replica.WithAutoRoleSwith(true),
		replica.WithLastIndex(lastIndex),
		replica.WithLastTerm(lastTerm),
	)
	c.rc = rc
	return c
}

func (c *channel) switchConfig(cfg wkdb.ChannelClusterConfig) error {
	c.mu.Lock()
	c.cfg = cfg
	c.mu.Unlock()

	c.Info("switch config", zap.String("cfg", cfg.String()))

	if cfg.Status == wkdb.ChannelClusterStatusCandidate {
		c.pausePropopose.Store(true)
	} else if cfg.Status == wkdb.ChannelClusterStatusNormal {
		c.pausePropopose.Store(false)
	}

	role := replica.RoleFollower

	if cfg.LeaderId == c.opts.NodeId {
		role = replica.RoleLeader
	}

	replicaCfg := replica.Config{
		MigrateFrom: cfg.MigrateFrom,
		MigrateTo:   cfg.MigrateTo,
		Replicas:    cfg.Replicas,
		Learners:    cfg.Learners,
		Version:     cfg.ConfVersion,
		Leader:      cfg.LeaderId,
		Role:        role,
		Term:        cfg.Term,
	}

	c.s.channelManager.channelReactor.Step(c.key, replica.Message{
		MsgType: replica.MsgConfigResp,
		Config:  replicaCfg,
	})

	return nil
}

func (c *channel) leaderId() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg.LeaderId
}

func (c *channel) term() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg.Term
}

func (c *channel) isLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg.LeaderId == c.opts.NodeId
}

func (c *channel) configVersion() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg.ConfVersion

}

// --------------------------IHandler-------------------------------

func (c *channel) LastLogIndexAndTerm() (uint64, uint32) {
	return c.rc.LastLogIndex(), c.rc.Term()
}

func (c *channel) HasReady() bool {
	return c.rc.HasReady()
}

func (c *channel) Ready() replica.Ready {
	return c.rc.Ready()
}

func (c *channel) GetLogs(startIndex uint64, endIndex uint64) ([]replica.Log, error) {
	return c.getLogs(startIndex, endIndex, uint64(c.opts.LogSyncLimitSizeOfEach))
}

func (c *channel) ApplyLogs(startIndex, endIndex uint64) (uint64, error) {
	return 0, nil
}

func (c *channel) AppliedIndex() (uint64, error) {
	return c.opts.MessageLogStorage.AppliedIndex(c.key)
}

func (c *channel) SetHardState(hd replica.HardState) {

	if hd.LeaderId == 0 {
		return
	}
	c.cfg.LeaderId = hd.LeaderId
	c.cfg.Term = hd.Term
	c.cfg.ConfVersion = hd.ConfVersion

	err := c.opts.ChannelClusterStorage.Save(c.cfg)
	if err != nil {
		c.Warn("save channel cluster config error", zap.Error(err))
	}
}

func (c *channel) Tick() {
	c.rc.Tick()

	if c.isLeader() {
		c.sendConfigTick++
		if c.sendConfigTick >= c.sendConfigTimeoutTick {
			if c.isLeader() {
				err := c.sendConfigReqToSlotLeader(c, c.cfg.ConfVersion)
				if err != nil {
					c.Error("send config req to slot leader error", zap.Error(err))
				}
			}
		}
	}

}

func (c *channel) Step(m replica.Message) error {
	return c.rc.Step(m)
}

func (c *channel) replicaCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cfg.Replicas)
}

func (c *channel) LeaderId() uint64 {
	return c.leaderId()
}

func (c *channel) SetSpeedLevel(level replica.SpeedLevel) {
	c.rc.SetSpeedLevel(level)
}

func (c *channel) SpeedLevel() replica.SpeedLevel {
	return c.rc.SpeedLevel()
}

func (c *channel) PausePropopose() bool {
	return c.pausePropopose.Load()
}

func (c *channel) SaveConfig(cfg replica.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cfg.MigrateFrom = cfg.MigrateFrom
	c.cfg.MigrateTo = cfg.MigrateTo
	c.cfg.Replicas = cfg.Replicas
	c.cfg.Learners = cfg.Learners
	c.cfg.ConfVersion = cfg.Version

	if c.cfg.MigrateFrom == 0 && c.cfg.MigrateTo == 0 {
		c.cfg.Status = wkdb.ChannelClusterStatusNormal
	}

	err := c.opts.ChannelClusterStorage.Save(c.cfg)
	if err != nil {
		c.Error("save channel cluster config error", zap.Error(err))
		return err
	}

	return nil
}

func (c *channel) AppendLogs(logs []replica.Log) error {
	return c.opts.MessageLogStorage.AppendLogs(c.key, logs)
}

func (c *channel) SetLeaderTermStartIndex(term uint32, index uint64) error {

	return c.opts.MessageLogStorage.SetLeaderTermStartIndex(c.key, term, index)
}

func (c *channel) LeaderTermStartIndex(term uint32) (uint64, error) {
	return c.opts.MessageLogStorage.LeaderTermStartIndex(c.key, term)
}

func (c *channel) LeaderLastTerm() (uint32, error) {
	return c.opts.MessageLogStorage.LeaderLastTerm(c.key)
}

func (c *channel) DeleteLeaderTermStartIndexGreaterThanTerm(term uint32) error {
	return c.opts.MessageLogStorage.DeleteLeaderTermStartIndexGreaterThanTerm(c.key, term)
}

func (c *channel) TruncateLogTo(index uint64) error {
	return c.opts.MessageLogStorage.TruncateLogTo(c.key, index)
}

func (c *channel) LearnerToFollower(learnerId uint64) error {
	c.Info("learner to  follower", zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType), zap.Uint64("learnerId", learnerId))

	return c.learnerTo(learnerId)
}

func (c *channel) LearnerToLeader(learnerId uint64) error {
	c.Info("learner to  leader", zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType), zap.Uint64("learnerId", learnerId))
	return c.learnerTo(learnerId)
}

func (c *channel) learnerTo(learnerId uint64) error {

	channelClusterCfg, err := c.opts.ChannelClusterStorage.Get(c.channelId, c.channelType)
	if err != nil {
		c.Error("onReplicaConfigChange failed", zap.Error(err), zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
		return err
	}
	if wkdb.IsEmptyChannelClusterConfig(channelClusterCfg) {
		return fmt.Errorf("LearnerToFollower: channel cluster config is empty")
	}

	if channelClusterCfg.MigrateFrom == 0 || channelClusterCfg.MigrateTo == 0 {
		return fmt.Errorf("LearnerToFollower: there is no migration")
	}

	if channelClusterCfg.MigrateTo != learnerId {
		c.Error("LearnerToFollower: learnerId is not equal to migrateTo", zap.Uint64("learnerId", learnerId), zap.Uint64("migrateTo", channelClusterCfg.MigrateTo))
		return fmt.Errorf("LearnerToFollower: learnerId is not equal to migrateTo")
	}

	channelClusterCfg.Learners = wkutil.RemoveUint64(channelClusterCfg.Learners, learnerId)
	channelClusterCfg.Replicas = wkutil.RemoveUint64(channelClusterCfg.Replicas, channelClusterCfg.MigrateFrom)
	channelClusterCfg.Replicas = append(channelClusterCfg.Replicas, learnerId)

	var learnerIsLeader = false // 学习者是新的领导者
	// 如果迁移的是领导节点，则将学习者设置为领导者
	if channelClusterCfg.MigrateFrom == c.leaderId() {
		channelClusterCfg.Term = channelClusterCfg.Term + 1
		channelClusterCfg.LeaderId = learnerId
		channelClusterCfg.Status = wkdb.ChannelClusterStatusNormal
		learnerIsLeader = true

	}
	channelClusterCfg.MigrateFrom = 0
	channelClusterCfg.MigrateTo = 0
	channelClusterCfg.ConfVersion = uint64(time.Now().UnixNano())

	// 如果是频道领导，则向槽领导提案最新的分布式配置
	if c.isLeader() {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.ProposeTimeout)
		defer cancel()
		err = c.opts.ChannelClusterStorage.Propose(timeoutCtx, channelClusterCfg)
		if err != nil {
			c.Error("propose channel cluster config failed", zap.Error(err), zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
			return err
		}
	} else {
		err = c.opts.ChannelClusterStorage.Save(channelClusterCfg)
		if err != nil {
			c.Error("update channel cluster config failed", zap.Error(err), zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
			return err
		}
	}

	// 生效配置
	err = c.switchConfig(channelClusterCfg)
	if err != nil {
		c.Error("LearnerToFollower: switch config failed", zap.Error(err), zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
		return err
	}

	// 如果是学习者是新的领导，则通知新领导更新配置
	if learnerIsLeader {
		err = c.s.sendChannelClusterConfigUpdate(channelClusterCfg.ChannelId, channelClusterCfg.ChannelType, channelClusterCfg.LeaderId)
		if err != nil {
			c.Error("LearnerToFollower: sendChannelClusterConfigUpdate failed", zap.Error(err), zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
			return err
		}
	}

	return nil
}

func (c *channel) getLogs(startLogIndex uint64, endLogIndex uint64, limitSize uint64) ([]replica.Log, error) {
	logs, err := c.opts.MessageLogStorage.Logs(c.key, startLogIndex, endLogIndex, limitSize)
	if err != nil {
		c.Error("get logs error", zap.Error(err))
		return nil, err
	}
	return logs, nil
}
