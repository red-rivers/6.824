package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	ReqId int64
	ClientId int64

	Key string
	Op string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	repChMap map[string]chan bool
	kvMap map[string]string
	clientReqMap map[int64]int64
	ClientId int64
	

	lastApplied int
	persister *raft.Persister
	// Your definitions here.
}

func (kv *KVServer) GetUUid (clientId, reqId int64) string {
	return fmt.Sprintf("%d-%d",clientId, reqId)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// DPrintf("[server %d] Get Call args : %+v", kv.me, args)
	op := Op {
		ClientId: args.ClientId,
		ReqId: args.ReqId,
		Key: args.Key,
	}
	repCh := make(chan bool, 1)
	rid, ok := kv.clientReqMap[args.ClientId]
	if ok && rid >= args.ReqId{
		repCh <- true
	} else {
		kv.rf.Start(op)
		kv.repChMap[kv.GetUUid(args.ClientId, args.ReqId)] = repCh	
	}
	kv.mu.Unlock()
	select {
	case <-repCh:{
		kv.mu.Lock()
		val, ok := kv.kvMap[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		}else {
			reply.Value = val
		}
		kv.mu.Unlock()
		break
	}
	case <-time.After(time.Duration(10)*time.Millisecond):{	
		reply.Err = ErrWrongLeader
		break
	}
	}
	// DPrintf("[server %d] Get call reply : %+v",kv.me, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// DPrintf("[server %d] PutAppend call args : %+v",kv.me ,args)
	op := Op{
		ReqId: args.ReqId,
		ClientId: args.ClientId,

		Key: args.Key,
		Op: args.Op,
		Value: args.Value,
	}
	if rid, ok := kv.clientReqMap[args.ClientId]; ok && rid>=args.ReqId {
		kv.mu.Unlock()
		return
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	repCh := make(chan bool, 1)
	kv.repChMap[kv.GetUUid(args.ClientId, args.ReqId)] = repCh
	kv.mu.Unlock()
	select {
	case <-repCh:{
		break
	}
	case <-time.After(time.Duration(10)*time.Millisecond):{	
		reply.Err = ErrWrongLeader
		break
	}
	}

	// DPrintf("[server %d] PutAppend call reply : %+v",kv.me, reply)
}

func (kv *KVServer) TackleApplyMsg() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		DPrintf("[server %d] revice applyMsg : %+v", kv.me, msg)
		if msg.SnapshotValid {
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			if d.Decode(&kv.kvMap) != nil || d.Decode(&kv.clientReqMap) != nil{
				DPrintf("[server %d] install snapshot err", kv.me)
			}
			kv.lastApplied = msg.SnapshotIndex
		}else {
			op := msg.Command.(Op)
			if repCh, ok := kv.repChMap[kv.GetUUid(op.ClientId, op.ReqId)]; ok {
				if len(repCh) == 1 {
					<-repCh
				}
				repCh <- true
			}
			if rid, ok := kv.clientReqMap[op.ClientId];!ok || rid < op.ReqId{
				if op.Op == "Put" || op.Op == "Append" {
					val := kv.kvMap[op.Key]
					if op.Op == "Put" {
						val = ""
					}
					val = val + op.Value
					kv.kvMap[op.Key] = val
				}
				kv.clientReqMap[op.ClientId] = op.ReqId
			}
			kv.lastApplied = msg.CommandIndex
			kv.SnapShot()
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) SnapShot() {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate{
		now := time.Now()
		DPrintf("[server %d] snapshot call start maxraftstate: %+v, RaftStateSize: %+v",kv.me ,kv.maxraftstate, kv.persister.RaftStateSize())
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.kvMap)
		e.Encode(kv.clientReqMap)
		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.lastApplied, snapshot)
		DPrintf("[server %d] snapshot call end maxraftstate: %+v, RaftStateSize: %+v, time cost : %+v, snapshot size : %+v",kv.me ,kv.maxraftstate, kv.persister.RaftStateSize(), time.Since(now), len(snapshot))
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.clientReqMap = make(map[int64]int64)
	kv.repChMap = make(map[string]chan bool)

	kv.kvMap = make(map[string]string)

	go kv.TackleApplyMsg()

	// You may need initialization code here.

	return kv
}
