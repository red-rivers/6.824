package shardctrler

import (
	"fmt"
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft

	configNum int
	applyCh chan raft.ApplyMsg
	repChMap map[string]chan bool
	clientReqMap map[int64]int64
	
	configs []Config // indexed by config num
}

const (
	Join = "Join"
	Leave = "Leave"
	Move = "Move"
	Query = "Query"
)


type Op struct {
	Type string
	Args interface{}
}

func (sc *ShardCtrler) GetUUid (clientId, reqId int64) string {
	return fmt.Sprintf("%d-%d",clientId, reqId)
}


func (sc *ShardCtrler) handler (args interface{}, uniqueReq UniqueReq, opType string) bool {
	sc.mu.Lock()
	DPrintf("[server %d] %s call args : %+v, config : %+v",sc.me, opType, args, sc.configs[sc.configNum])
	op := Op {
		Type: opType,
		Args: args,
	}
	if rid, ok := sc.clientReqMap[uniqueReq.ClientId]; ok && rid>=uniqueReq.ReqId {
		sc.mu.Unlock()
		return true
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		return false
	}
	repCh := make(chan bool, 1)
	sc.repChMap[sc.GetUUid(uniqueReq.ClientId, uniqueReq.ReqId)] = repCh
	sc.mu.Unlock()
	select {
	case <-repCh:{
		sc.mu.Lock()
		DPrintf("[server %d] %s call success, config : %+v",sc.me ,opType, sc.configs[sc.configNum])
		sc.mu.Unlock()
		return true
	}
	case <-time.After(time.Duration(100)*time.Millisecond):{
		return false	
	}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	ok := sc.handler(*args, args.UniqueReq, Join)
	if ok {
		reply.WrongLeader = false
	}else {
		reply.WrongLeader = true
	}
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	ok := sc.handler(*args, args.UniqueReq, Leave)
	if ok {
		reply.WrongLeader = false
	}else {
		reply.WrongLeader = true
	}
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	ok := sc.handler(*args, args.UniqueReq, Move)
	if ok {
		reply.WrongLeader = false
	}else {
		reply.WrongLeader = true
	}
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	ok := sc.handler(*args, args.UniqueReq, Query)
	if ok {
		sc.mu.Lock()
		reply.WrongLeader = false
		if args.Num == -1 {
			reply.Config = sc.configs[sc.configNum]
		}else if args.Num >= 0 && args.Num <= sc.configNum{
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()	
	}else {
		reply.WrongLeader = true
	}
	return
}

func (sc *ShardCtrler) actuator () {
	for msg := range sc.applyCh {
		op, _ := msg.Command.(Op)
		sc.mu.Lock()
		DPrintf("[server %d] actuator recivve applyMsg : %+v",sc.me, op)
		switch op.Type {
		case Join :{
			sc.executeJoin(op.Args)
		}
		case Leave :{
			sc.executeLeave(op.Args)
		}
		case Move :{
			sc.executeMove(op.Args)
		}
		case Query:{
			sc.executeQuery(op.Args)	
		}
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) group2shard(config Config) map[int][]int {
	groupToShard := make(map[int][]int)
	for gid, _ := range config.Groups {
		groupToShard[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		if gid != 0{
			groupToShard[gid] = append(groupToShard[gid], shard);
		}
	} 
	return groupToShard
}

func (sc *ShardCtrler) getGroup(config Config, loaded bool, groupToShard map[int][]int) int {
	val := 0
	var shard int
	if loaded {
		shard = -1
	}else {
		shard = NShards+1
	}
	groups := make([]int, 0)
	for gid, _ := range groupToShard {
		groups = append(groups, gid)
	}
	min := func (a, b int) int {
		if a < b {
			return a
		}
		return b
	}
	for _, gid := range groups {
		if loaded && len(groupToShard[gid]) >= shard {
			if len(groupToShard[gid]) == shard {
				val = min(val, gid)
			}else {
				shard = len(groupToShard[gid])
				val = gid
			}
		}
		if !loaded && len(groupToShard[gid]) <= shard{
			if len(groupToShard[gid]) == shard {
				val = min(val, gid)
			}else {
				shard = len(groupToShard[gid])
				val = gid
			}
		}
	}
	return val
}

func (sc *ShardCtrler) rebalance(config *Config, group2shard map[int][]int) {
	for shard, gid := range config.Shards {
		if (gid == 0) {
			unloadedGid := sc.getGroup(*config, false, group2shard)
			group2shard[unloadedGid] = append(group2shard[unloadedGid], shard)
			config.Shards[shard] = unloadedGid
		}
	}
	for ;; {
		loadedGid := sc.getGroup(*config, true, group2shard)
		unLoadedGid := sc.getGroup(*config, false, group2shard)
		if len(group2shard[loadedGid]) <= len(group2shard[unLoadedGid])+1 || loadedGid == unLoadedGid {
			break
		}
		if len(group2shard[loadedGid]) > 0 {
			moveShard := group2shard[loadedGid][0]
			group2shard[loadedGid] = group2shard[loadedGid][1:]
			group2shard[unLoadedGid] = append(group2shard[unLoadedGid], moveShard)
			config.Shards[moveShard] = unLoadedGid
		}
	}
}

func (sc *ShardCtrler) executeJoin(args interface{}) {
	sc.deepCopy()
	config := &sc.configs[sc.configNum]
	info := args.(JoinArgs)

	for gid, servers := range info.Servers {
		config.Groups[gid] = servers
	}
	group2shard := sc.group2shard(*config)
	sc.rebalance(config, group2shard)
	sc.reactReq(info.UniqueReq)
}

func (sc *ShardCtrler) executeLeave(args interface{}) {
	sc.deepCopy()
	config := &sc.configs[sc.configNum]
	info := args.(LeaveArgs)

	leaveMap := make(map[int]bool)
	for _, gid := range info.GIDs {
		leaveMap[gid] = true
		delete(config.Groups, gid)
	}

	for shard, gid := range config.Shards {
		if _, ok := leaveMap[gid]; ok {
			config.Shards[shard] = 0
		}
	}
	group2shard := sc.group2shard(*config)
	sc.rebalance(config, group2shard)
	sc.reactReq(info.UniqueReq)
}


func (sc *ShardCtrler) executeMove(args interface{}) {
	sc.deepCopy()
	config := &sc.configs[sc.configNum]
	info := args.(MoveArgs)
	
	config.Shards[info.Shard] = info.GID
	group2shard := sc.group2shard(*config)
	sc.rebalance(config, group2shard)
	sc.reactReq(info.UniqueReq)
}

func (sc *ShardCtrler) executeQuery(args interface{}) {
	info := args.(QueryArgs)
	sc.reactReq(info.UniqueReq)
}

func (sc *ShardCtrler) reactReq(uniReq UniqueReq) {
	if repCh, ok := sc.repChMap[sc.GetUUid(uniReq.ClientId, uniReq.ReqId)]; ok {
		if len(repCh) == 1 {
			<-repCh
		}
		repCh <- true
	}
}

func (sc *ShardCtrler) deepCopy() {
	lastConfig := sc.configs[sc.configNum]
	sc.configNum++
	config := Config{
		Num: sc.configNum,
	}
	config.Groups = make(map[int][]string)
	for g, s := range lastConfig.Groups{
		ns := make([]string, 0)
		ns = append(ns, s...)
		config.Groups[g] = ns
	}
	for shard, gid := range lastConfig.Shards {
		config.Shards[shard] = gid
	}
	sc.configs = append(sc.configs, config)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	registerLabgob()
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.clientReqMap = make(map[int64]int64)
	sc.repChMap = make(map[string]chan bool)

	go sc.actuator()

	return sc
}

func registerLabgob() {
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
}

func checkFunction () {

}