package kvraft

import (
	"crypto/rand"

	"6.824/labrpc"

	"math/big"
	rand1 "math/rand"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	uuid := nrand()
	value := ""
	for ;; {
		server := rand1.Int()%len(ck.servers)
		args := &GetArgs{
			UUid: uuid,
			Key: key,
		}
		reply := &GetReply{}
		if ok := ck.servers[server].Call("KVServer.Get", args, reply); ok && reply.Err != ErrWrongLeader{
			value = reply.Value
			break
		}
	}
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	uuid := nrand()
	for ;; {
		server := rand1.Int()%len(ck.servers)
		args := &PutAppendArgs{
			UUid: uuid,
			Key: key,
			Op: op,
			Value: value,
		}
		reply := &PutAppendReply{}
		if ok := ck.servers[server].Call("KVServer.PutAppend", args, reply); ok && reply.Err == ""{
			break
		}
	}
}

// func (ck *Clerk) GetUUid () string {
// 	uuid, _ := exec.Command("uuidgen").Output()
// 	return string(uuid)
// }

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
