package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
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


type Op struct {
	UUid int64
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
	applyMap map[int64]bool
	repChMap map[int64]chan bool
	kvMap map[string]string
	

	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	DPrintf("[server %d] Get Call args : %+v", kv.me, args)
	op := Op {
		UUid: args.UUid,
		Key: args.Key,
	}
	repCh := make(chan bool, 1)
	if ok := kv.applyMap[args.UUid]; !ok {
		kv.rf.Start(op)
		kv.repChMap[args.UUid] = repCh	
	} else {
		repCh <- true
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
	case <-time.After(time.Duration(50)*time.Millisecond):{	
		reply.Err = ErrWrongLeader
		break
	}
	}
	DPrintf("[server %d] Get call reply : %+v",kv.me, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	DPrintf("[server %d] PutAppend call args : %+v",kv.me ,args)
	op := Op{
		UUid: args.UUid,
		Key: args.Key,
		Op: args.Op,
		Value: args.Value,
	}
	if ok := kv.applyMap[args.UUid]; ok{
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
	kv.repChMap[args.UUid] = repCh
	kv.mu.Unlock()
	select {
	case <-repCh:{
		break
	}
	case <-time.After(time.Duration(50)*time.Millisecond):{	
		reply.Err = ErrWrongLeader
		delete(kv.repChMap, args.UUid)
		break
	}
	}

	DPrintf("[server %d] PutAppend call reply : %+v",kv.me, reply)
}

func (kv *KVServer) TackleApplyMsg() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		DPrintf("[server %d] revice applyMsg : %+v", kv.me, msg)
		op := msg.Command.(Op)
		if repCh, ok := kv.repChMap[op.UUid]; ok {
			if len(repCh) == 1 {
				<-repCh
			}
			repCh <- true
		}
		if _, ok := kv.applyMap[op.UUid]; !ok {
			if op.Op == "Put" || op.Op == "Append" {
				val := kv.kvMap[op.Key]
				if op.Op == "Put" {
					val = ""
				}
				val = val + op.Value
				kv.kvMap[op.Key] = val
			}
			kv.applyMap[op.UUid] = true
		}
		kv.mu.Unlock()
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.applyMap = make(map[int64]bool)
	kv.repChMap = make(map[int64]chan bool)

	kv.kvMap = make(map[string]string)

	go kv.TackleApplyMsg()

	// You may need initialization code here.

	return kv
}
