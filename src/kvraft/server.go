package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	Debug          = false
	RequestTimeout = 1 * time.Second
)

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
	Type      string // "Put", "Append" or "Get"
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db                map[string]string // key-value database
	applyCond         *sync.Cond
	lastAppliedIndex  int               // last applied index
	lastAppliedTerm   int               // last applied term
	clientLastRequest map[int64]Request // client last requests
}

type Request struct {
	RequestId int64
	Value     string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.applyCond.L.Lock()
	req, ok := kv.clientLastRequest[args.ClientId]
	kv.applyCond.L.Unlock()

	if ok && req.RequestId == args.RequestId {
		reply.Err = OK
		reply.Value = req.Value
		return
	}

	op := Op{
		Type:      "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// DPrintf("server %d start: %v index: %v term: %v", kv.me, op, index, term)

	success := kv.waitApply(index, term)
	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	kv.applyCond.L.Lock()
	defer kv.applyCond.L.Unlock()

	reply.Err = OK
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.applyCond.L.Lock()
	req, ok := kv.clientLastRequest[args.ClientId]
	kv.applyCond.L.Unlock()

	if ok && req.RequestId == args.RequestId {
		reply.Err = OK
		return
	}

	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// DPrintf("server %d start: %v index: %v term: %v", kv.me, op, index, term)

	success := kv.waitApply(index, term)
	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
}

func (kv *KVServer) waitApply(index int, term int) bool {
	kv.applyCond.L.Lock()
	defer kv.applyCond.L.Unlock()

	for !kv.killed() && kv.lastAppliedIndex < index {
		kv.applyCond.Wait()
	}

	return kv.lastAppliedIndex >= index && kv.lastAppliedTerm == term
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.applyCond = sync.NewCond(&kv.mu)
	kv.lastAppliedIndex = 0
	kv.lastAppliedTerm = 0
	kv.clientLastRequest = make(map[int64]Request)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyListener()

	return kv
}

func (kv *KVServer) applyListener() {
	for msg := range kv.applyCh {
		DPrintf("server %d receive apply msg: %v", kv.me, msg)
		kv.applyCond.L.Lock()
		if msg.CommandValid && msg.CommandIndex > kv.lastAppliedIndex {
			op := msg.Command.(Op)
			req, ok := kv.clientLastRequest[op.ClientId]
			if !(ok && req.RequestId == op.RequestId) {
				request := Request{}
				switch op.Type {
				case "Get":
					request = Request{
						RequestId: op.RequestId,
						Value:     kv.db[op.Key],
					}
				case "Put":
					kv.db[op.Key] = op.Value
					request = Request{
						RequestId: op.RequestId,
						Value:     "",
					}
				case "Append":
					kv.db[op.Key] += op.Value
					request = Request{
						RequestId: op.RequestId,
						Value:     "",
					}
				}
				kv.clientLastRequest[op.ClientId] = request
			}

			kv.lastAppliedIndex = msg.CommandIndex
			kv.lastAppliedTerm = msg.CommandTerm
			kv.applyCond.Broadcast()
		}
		kv.applyCond.L.Unlock()
	}
}
