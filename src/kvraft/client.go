package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clientId int64
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
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: nrand(),
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
			done := make(chan bool)
			go func(serverId int) {
				// DPrintf("Request Get: serverId %v, args %v, ", serverId, args)
				done <- ck.servers[serverId].Call("KVServer.Get", &args, &reply)
				// DPrintf("Request Get: serverId %v, args %v, reply %v", serverId, args, reply.Err)
			}(serverId)

			select {
			case ok := <-done:
				if !ok {
					continue
				}
				switch reply.Err {
				case OK:
					ck.leaderId = serverId
					return reply.Value
				case ErrNoKey:
					ck.leaderId = serverId
					return ""
				case ErrWrongLeader:
					ck.leaderId = -1
				default:
				}
			case <-time.After(RequestTimeout):
				// DPrintf("Request Get: serverId %v, args %v, timeout", serverId, args)
			}
		}
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
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: nrand(),
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
			done := make(chan bool)
			go func(serverId int) {
				// DPrintf("Request PutAppend: serverId %v, args %v, ", serverId, args)
				done <- ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
				// DPrintf("Request PutAppend: serverId %v, args %v, reply %v", serverId, args, reply.Err)
			}(serverId)

			select {
			case ok := <-done:
				if !ok {
					continue
				}
				switch reply.Err {
				case OK:
					ck.leaderId = serverId
					return
				case ErrWrongLeader:
					ck.leaderId = -1
				default:
				}
			case <-time.After(RequestTimeout):
				// DPrintf("Request PutAppend: serverId %v, args %v, timeout", serverId, args)
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
