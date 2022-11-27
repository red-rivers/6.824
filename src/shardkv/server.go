package shardkv

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ShardKV struct {
	mu      sync.RWMutex
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	makeEnd func(string) *labrpc.ClientEnd
	gid     int
	sc      *shardctrler.Clerk

	maxRaftState int // snapshot if log grows this big
	lastApplied  int // record the lastApplied to prevent stateMachine from rollback

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config

	stateMachines  map[int]*Shard                // KV stateMachines
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
}


func (kv *ShardKV) Command(request *CommandRequest, response *CommandResponse) {
	kv.mu.RLock()
	// return result directly without raft layer's participation if request is duplicated
	if request.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.lastOperations[request.ClientId].LastResponse
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.RUnlock()
		return
	}
	// return ErrWrongGroup directly to let client fetch latest configuration and perform a retry if this key can't be served by this shard at present
	if !kv.canServe(key2shard(request.Key)) {
		response.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.Execute(NewOperationCommand(request), response)
}

func (kv *ShardKV) GetShardsData(request *ShardOperationRequest, response *ShardOperationResponse) {
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	defer DPrintf("{Node %v}{Group %v} processes PullTaskRequest %v with response %v", kv.rf.Me(), kv.gid, request, response)

	if kv.currentConfig.Num < request.ConfigNum {
		response.Err = ErrNotReady
		return
	}

	response.Shards = make(map[int]map[string]string)
	for _, shardID := range request.ShardIDs {
		response.Shards[shardID] = kv.stateMachines[shardID].deepCopy()
	}

	response.LastOperations = make(map[int64]OperationContext)
	for clientID, operation := range kv.lastOperations {
		response.LastOperations[clientID] = operation.deepCopy()
	}

	response.ConfigNum, response.Err = request.ConfigNum, OK
}

func (kv *ShardKV) DeleteShardsData(request *ShardOperationRequest, response *ShardOperationResponse) {
	// only delete shards when role is leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	defer DPrintf("{Node %v}{Group %v} processes GCTaskRequest %v with response %v", kv.rf.Me(), kv.gid, request, response)

	kv.mu.RLock()
	if kv.currentConfig.Num > request.ConfigNum {
		DPrintf("{Node %v}{Group %v}'s encounters duplicated shards deletion %v when currentConfig is %v", kv.rf.Me(), kv.gid, request, kv.currentConfig)
		response.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	var commandResponse CommandResponse
	kv.Execute(NewDeleteShardsCommand(request), &commandResponse)

	response.Err = commandResponse.Err
}

func (kv *ShardKV) Execute(command Command, response *CommandResponse) {
	kv.mu.Lock()
	commandIndex, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	notifyChan := make(chan *CommandResponse)
	kv.notifyChans[commandIndex] = notifyChan
	kv.mu.Unlock()
	select {
	case response = <-notifyChan: {
		break
	}
	case <-time.After(time.Duration(50)*time.Millisecond):{
		response.Err = ErrWrongLeader
		break
	}
	}
}

func (kv *ShardKV) updateShardStatus(config *shardctrler.Config) {
	for shardId, _ := range kv.currentConfig.Shards {
		if kv.lastConfig.Shards[shardId] != kv.gid && kv.currentConfig.Shards[shardId] == kv.gid {
			kv.stateMachines[shardId] = NewShard()
			kv.stateMachines[shardId].Status = Pulling
		} 
		if kv.lastConfig.Shards[shardId] == kv.gid && kv.currentConfig.Shards[shardId] != kv.gid {
			kv.stateMachines[shardId].Status = BePulling
		}
	}
}

func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for shardId, gid := range kv.currentConfig.Shards {
		if gid == kv.gid {
			if kv.stateMachines[shardId].Status == status {
				if gid2shardIDs[gid] == nil {
					gid2shardIDs[gid] = make([]int, 0)
				}
				gid2shardIDs[gid] = append(gid2shardIDs[gid], shardId)
			}
		}
	}
	return gid2shardIDs
}

func (kv *ShardKV) isDuplicateRequest(clientId, commandId int64) bool {
	return kv.lastOperations[clientId].MaxAppliedCommandId >= commandId
}

// check whether this raft group can serve this shard at present
func (kv *ShardKV) canServe(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid && (kv.stateMachines[shardID].Status == Serving || kv.stateMachines[shardID].Status == GCing)
}

func (kv *ShardKV) getNotifyChan(commandIndex int) chan *CommandResponse{
	return kv.notifyChans[commandIndex]
}

func (kv *ShardKV) needSnapshot() bool {
	return false
}

func (kv *ShardKV) takeSnapshot(commandIndex int) {

}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	
}


// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *ShardKV) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			DPrintf("{Node %v}{Group %v} tries to apply message %v", kv.rf.Me(), kv.gid, message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v}{Group %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), kv.gid, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response *CommandResponse
				command := message.Command.(Command)
				switch command.Op {
				case Operation:
					operation := command.Data.(CommandRequest)
					response = kv.applyOperation(&message, &operation)
				case Configuration:
					nextConfig := command.Data.(shardctrler.Config)
					response = kv.applyConfiguration(&nextConfig)
				case InsertShards:
					shardsInfo := command.Data.(ShardOperationResponse)
					response = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					shardsInfo := command.Data.(ShardOperationRequest)
					response = kv.applyDeleteShards(&shardsInfo)
				// case EmptyEntry:
				// 	response = kv.applyEmptyEntry()
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CommandRequest{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationResponse{})
	labgob.Register(ShardOperationRequest{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		makeEnd:        makeEnd,
		gid:            gid,
		sc:             shardctrler.MakeClerk(ctrlers),
		lastApplied:    0,
		maxRaftState:   maxRaftState,
		currentConfig:  shardctrler.DefaultConfig(),
		lastConfig:     shardctrler.DefaultConfig(),
		stateMachines:  make(map[int]*Shard),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandResponse),
	}
	kv.restoreSnapshot(persister.ReadSnapshot())
	// start applier goroutine to apply committed logs to stateMachine
	go kv.applier()
	// start configuration monitor goroutine to fetch latest configuration
	go kv.Monitor(kv.configureAction, ConfigureMonitorTimeout)
	// start migration monitor goroutine to pull related shards
	go kv.Monitor(kv.migrationAction, MigrationMonitorTimeout)
	// start gc monitor goroutine to delete useless shards in remote groups
	go kv.Monitor(kv.gcAction, GCMonitorTimeout)
	// start entry-in-currentTerm monitor goroutine to advance commitIndex by appending empty entries in current term periodically to avoid live locks
	// go kv.Monitor(kv.checkEntryInCurrentTermAction, EmptyEntryDetectorTimeout)

	DPrintf("{Node %v}{Group %v} has started", kv.rf.Me(), kv.gid)
	return kv
}

func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}