package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int
	clientId  int64
	requestId int64
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

	ck.leaderId = -1
	ck.clientId = nrand()
	ck.requestId = 0

	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.requestId++
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}

	for {
		serversToTry := []int{ck.leaderId}
		if ck.leaderId == -1 {
			serversToTry = make([]int, len(ck.servers))
			for i := range ck.servers {
				serversToTry[i] = i
			}
		}

		for _, serverId := range serversToTry {
			reply := GetReply{}
			ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			switch reply.Err {
			case OK:
				ck.leaderId = serverId
				// DPrintf("Get: key %v, value %v", key, reply.Value)
				return reply.Value
			case ErrNoKey:
				ck.leaderId = serverId
				// DPrintf("Get: key %v does not exist", key)
				return ""
			case ErrWrongLeader:
				// DPrintf("Get: wrong leader %v", serverId)
			default:
				// DPrintf("Get: unexpected error %v", reply.Err)
			}
		}

		time.Sleep(100 * time.Millisecond)
		ck.leaderId = -1
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestId++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}

	for {
		serversToTry := []int{ck.leaderId}
		if ck.leaderId == -1 {
			serversToTry = make([]int, len(ck.servers))
			for i := range ck.servers {
				serversToTry[i] = i
			}
		}

		for _, serverId := range serversToTry {
			reply := PutAppendReply{}
			ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
			// DPrintf("PutAppend: serverId %v, ok %v, args %v, reply %v", serverId, ok, args, reply)
			if !ok {
				continue
			}
			switch reply.Err {
			case OK:
				ck.leaderId = serverId
				// DPrintf("PutAppend: key %v, value %v", key, value)
				return
			case ErrWrongLeader:
				// DPrintf("PutAppend: wrong leader %v", serverId)
			default:
				// DPrintf("PutAppend: unexpected error %v", reply.Err)
			}
		}

		time.Sleep(100 * time.Millisecond)
		ck.leaderId = -1
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
