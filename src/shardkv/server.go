package shardkv

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	Debug             = false
	PersistTimePeriod = 100 * time.Millisecond
	MonitorTimePeriod = 100 * time.Millisecond
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
	Type              string // "Put", "Append", "Get", "Config", "MoveIn", "MoveOut"
	Key               string
	Value             string
	ClientId          int64
	RequestId         int64
	Config            shardctrler.Config
	Shard             int
	Data              []byte
	ClientLastRequest map[int64]Request
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applyCond                *sync.Cond
	mck                      *shardctrler.Clerk
	isLatestConfig           bool
	config                   shardctrler.Config
	shards                   map[int]struct{}          // shards
	addShards                map[int]struct{}          // migrating shards(add shards)
	delShards                map[int]struct{}          // migrating shards(del shards)
	shardDB                  map[int]map[string]string // shard key-value database
	lastAppliedIndex         int                       // last applied index
	lastAppliedTerm          int                       // last applied term
	clientLastRequest        map[int64]Request         // client last requests
	shardProgressingRequests map[int][]int64           // shard progressing requests
}

type Request struct {
	RequestId int64
	Value     string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)

	kv.applyCond.L.Lock()
	if !kv.isLatestConfig {
		reply.Err = ErrShardMigrating
		kv.applyCond.L.Unlock()
		return
	}
	if _, ok := kv.shards[shard]; !ok {
		reply.Err = ErrWrongGroup
		kv.applyCond.L.Unlock()
		return
	}
	if _, ok := kv.addShards[shard]; ok {
		reply.Err = ErrShardMovingIn
		kv.applyCond.L.Unlock()
		return
	}
	if _, ok := kv.delShards[shard]; ok {
		reply.Err = ErrShardMovingOut
		kv.applyCond.L.Unlock()
		return
	}
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
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := kv.rf.Start(op)
	// DPrintf("G%d S%d S%d Get, index: %v term: %v",
	// 	kv.gid, kv.me, shard, index, term)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.applyCond.L.Lock()
	kv.shardProgressingRequests[shard] = append(kv.shardProgressingRequests[shard], op.RequestId)
	kv.applyCond.L.Unlock()

	success := kv.waitApply(index, term)
	// DPrintf("G%d S%d S%d Get: %v, index: %v term: %v",
	// kv.gid, kv.me, shard, success, index, term)

	kv.applyCond.L.Lock()
	defer kv.applyCond.L.Unlock()

	kv.shardProgressingRequests[shard] = removeInt64(kv.shardProgressingRequests[shard], op.RequestId)

	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = kv.shardDB[shard][args.Key]
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)

	kv.applyCond.L.Lock()
	if !kv.isLatestConfig {
		reply.Err = ErrShardMigrating
		kv.applyCond.L.Unlock()
		return
	}
	if _, ok := kv.shards[shard]; !ok {
		reply.Err = ErrWrongGroup
		kv.applyCond.L.Unlock()
		return
	}
	if _, ok := kv.addShards[shard]; ok {
		reply.Err = ErrShardMovingIn
		kv.applyCond.L.Unlock()
		return
	}
	if _, ok := kv.delShards[shard]; ok {
		reply.Err = ErrShardMovingOut
		kv.applyCond.L.Unlock()
		return
	}
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
	// DPrintf("G%d S%d S%d PutAppend, index: %v term: %v",
	// 	kv.gid, kv.me, shard, index, term)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.applyCond.L.Lock()
	kv.shardProgressingRequests[shard] = append(kv.shardProgressingRequests[shard], op.RequestId)
	kv.applyCond.L.Unlock()

	success := kv.waitApply(index, term)
	// DPrintf("G%d S%d S%d PutAppend: %v, Op: %v index: %v term: %v",
	// 	kv.gid, kv.me, shard, success, op, index, term)

	kv.applyCond.L.Lock()
	defer kv.applyCond.L.Unlock()

	kv.shardProgressingRequests[shard] = removeInt64(kv.shardProgressingRequests[shard], op.RequestId)

	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
}

func (kv *ShardKV) MoveIn(args *MoveInArgs, reply *MoveInReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.applyCond.L.Lock()
	if kv.config.Num > args.Num {
		reply.Err = OK
		kv.applyCond.L.Unlock()
		return
	}
	if kv.config.Num < args.Num {
		DPrintf("G%d S%d MoveIn, Num: %d, Args.Num: %d", kv.gid, kv.me, kv.config.Num, args.Num)
		reply.Err = ErrWrongConfig
		kv.applyCond.L.Unlock()
		return
	}
	if _, ok := kv.addShards[args.Shard]; !ok {
		reply.Err = OK
		kv.applyCond.L.Unlock()
		return
	}
	req, ok := kv.clientLastRequest[args.ClientId]
	kv.applyCond.L.Unlock()

	if ok && req.RequestId == args.RequestId {
		reply.Err = OK
		return
	}

	op := Op{
		Type:              "MoveIn",
		ClientId:          args.ClientId,
		RequestId:         args.RequestId,
		Shard:             args.Shard,
		Data:              args.Data,
		ClientLastRequest: args.ClientLastRequest,
	}

	index, term, isLeader := kv.rf.Start(op)
	// DPrintf("G%d S%d receive shard %d", kv.gid, kv.me, args.Shard)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	success := kv.waitApply(index, term)
	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	// DPrintf("G%d S%d receive shard %d MoveIn ok, addShards: %+v delShards: %+v",
	// 	kv.gid, kv.me, args.Shard, kv.addShards, kv.delShards)

	reply.Err = OK
}

func (kv *ShardKV) sync(op Op) bool {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	return kv.waitApply(index, term)
}

func (kv *ShardKV) waitApply(index int, term int) bool {
	kv.applyCond.L.Lock()
	defer kv.applyCond.L.Unlock()

	for kv.lastAppliedIndex < index {
		kv.applyCond.Wait()
	}

	return kv.lastAppliedIndex >= index && kv.lastAppliedTerm == term
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MoveInArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applyCond = sync.NewCond(&kv.mu)
	kv.shards = make(map[int]struct{})
	kv.addShards = make(map[int]struct{})
	kv.delShards = make(map[int]struct{})
	kv.shardDB = make(map[int]map[string]string)
	kv.lastAppliedIndex = 0
	kv.lastAppliedTerm = 0
	kv.clientLastRequest = make(map[int64]Request)
	kv.shardProgressingRequests = make(map[int][]int64)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.readPersist(persister.ReadSnapshot())
	go kv.cronPersist(persister)
	go kv.applyListener()
	go kv.monitorConfig()
	// go kv.configListener()

	return kv
}

func (kv *ShardKV) applyListener() {
	for msg := range kv.applyCh {
		if !msg.CommandValid && msg.SnapshotValid {
			kv.readPersist(msg.Snapshot)
		}

		kv.applyCond.L.Lock()
		if msg.CommandValid && msg.CommandIndex > kv.lastAppliedIndex {
			op := msg.Command.(Op)
			req, ok := kv.clientLastRequest[op.ClientId]
			if !(ok && req.RequestId == op.RequestId) {
				request := Request{RequestId: op.RequestId}
				shard := key2shard(op.Key)
				switch op.Type {
				case "Get":
					request = Request{
						RequestId: op.RequestId,
						Value:     kv.shardDB[shard][op.Key],
					}
				case "Put":
					if _, ok := kv.shardDB[shard]; !ok {
						kv.shardDB[shard] = make(map[string]string)
					}
					kv.shardDB[shard][op.Key] = op.Value
				case "Append":
					if _, ok := kv.shardDB[shard]; !ok {
						kv.shardDB[shard] = make(map[string]string)
					}
					kv.shardDB[shard][op.Key] += op.Value
				case "Config":
					if op.Config.Num == kv.config.Num+1 {
						if op.Config.Num > 1 {
							oldShards := genShards(kv.gid, kv.config.Shards)
							newShards := genShards(kv.gid, op.Config.Shards)
							addShards := make(map[int]struct{})
							delShards := make(map[int]struct{})
							for k := range newShards {
								if _, exists := oldShards[k]; !exists {
									addShards[k] = struct{}{}
								}
							}
							for k := range oldShards {
								if _, exists := newShards[k]; !exists {
									delShards[k] = struct{}{}
								}
							}
							kv.addShards = addShards
							kv.delShards = delShards
						}
						kv.shards = genShards(kv.gid, op.Config.Shards)
						kv.config = op.Config
						DPrintf("G%d S%d Config %d", kv.gid, kv.me, op.Config.Num)
					}
				case "MoveIn":
					r := bytes.NewBuffer(op.Data)
					d := labgob.NewDecoder(r)
					var db map[string]string
					if d.Decode(&db) != nil {
						DPrintf("G%d S%d MoveIn shard %d failed", kv.gid, kv.me, op.Shard)
					} else {
						delete(kv.addShards, op.Shard)
						kv.shardDB[op.Shard] = db
						for k, v := range op.ClientLastRequest {
							kv.clientLastRequest[k] = v
						}
						DPrintf("G%d S%d MoveIn shard %d", kv.gid, kv.me, op.Shard)
					}
				case "MoveOut":
					delete(kv.delShards, op.Shard)
					delete(kv.shardDB, op.Shard)
					DPrintf("G%d S%d MoveOut shard %d", kv.gid, kv.me, op.Shard)
				}
				kv.clientLastRequest[op.ClientId] = request
			}
			kv.lastAppliedIndex = msg.CommandIndex
			kv.lastAppliedTerm = msg.CommandTerm
		}
		kv.applyCond.L.Unlock()

		kv.applyCond.Broadcast()
	}
}

func (kv *ShardKV) persist() {
	if kv.maxraftstate < 0 {
		return
	}

	kv.applyCond.L.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.config)
	e.Encode(kv.shards)
	e.Encode(kv.addShards)
	e.Encode(kv.delShards)
	e.Encode(kv.shardDB)
	e.Encode(kv.lastAppliedIndex)
	e.Encode(kv.lastAppliedTerm)
	e.Encode(kv.clientLastRequest)
	snapshot := w.Bytes()
	kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
	kv.applyCond.L.Unlock()
}

func (kv *ShardKV) cronPersist(persister *raft.Persister) {
	for {
		if kv.maxraftstate != -1 && persister.RaftStateSize() > kv.maxraftstate {
			kv.persist()
		}
		time.Sleep(PersistTimePeriod)
	}
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	kv.applyCond.L.Lock()
	defer kv.applyCond.L.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var config shardctrler.Config
	var shards map[int]struct{}
	var addShards map[int]struct{}
	var delShards map[int]struct{}
	var shardDB map[int]map[string]string
	var lastAppliedIndex int
	var lastAppliedTerm int
	var clientLastRequest map[int64]Request
	if d.Decode(&config) != nil ||
		d.Decode(&shards) != nil ||
		d.Decode(&addShards) != nil ||
		d.Decode(&delShards) != nil ||
		d.Decode(&shardDB) != nil ||
		d.Decode(&lastAppliedIndex) != nil ||
		d.Decode(&lastAppliedTerm) != nil ||
		d.Decode(&clientLastRequest) != nil {
		return
	} else {
		kv.config = config
		kv.shards = shards
		kv.addShards = addShards
		kv.delShards = delShards
		kv.shardDB = shardDB
		kv.lastAppliedIndex = lastAppliedIndex
		kv.lastAppliedTerm = lastAppliedTerm
		kv.clientLastRequest = clientLastRequest
		DPrintf("G%d S%d read persist, N%d shards: %+v addShards: %+v delShards: %+v",
			kv.gid, kv.me, kv.config.Num, kv.shards, kv.addShards, kv.delShards)
	}
}

func (kv *ShardKV) monitorConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(MonitorTimePeriod)
			continue
		}

		latestConfig := kv.mck.Query(-1)

		kv.applyCond.L.Lock()
		currentConfigNum := kv.config.Num
		if currentConfigNum < latestConfig.Num {
			kv.isLatestConfig = false
		} else {
			kv.isLatestConfig = true
		}
		kv.applyCond.L.Unlock()

		for i := currentConfigNum + 1; i <= latestConfig.Num; i++ {
			for {
				kv.applyCond.L.Lock()
				if _, isLeader := kv.rf.GetState(); !isLeader {
					kv.applyCond.L.Unlock()
					break
				}
				DPrintf("S%d G%d N%d, shards: %+v addShards: %+v delShards: %+v",
					kv.me, kv.gid, kv.config.Num, kv.shards, kv.addShards, kv.delShards)
				if len(kv.delShards) > 0 {
					var configC shardctrler.Config
					var delShards map[int]struct{}
					deepCopyViaGob(&kv.config, &configC)
					delShards = copyMap(kv.delShards)
					kv.applyCond.L.Unlock()
					kv.sendMoveIn(configC, delShards)
					break
				}
				if len(kv.addShards) == 0 && len(kv.delShards) == 0 {
					kv.applyCond.L.Unlock()
					break
				}
				kv.applyCond.L.Unlock()
				time.Sleep(MonitorTimePeriod)
			}

			newConfig := kv.mck.Query(i)
			op := Op{
				Type:      "Config",
				Config:    newConfig,
				ClientId:  int64(kv.gid),
				RequestId: nrand(),
			}
			if !kv.sync(op) {
				break
			}

			var isNeedSend bool
			var configC shardctrler.Config
			var delShards map[int]struct{}
			kv.applyCond.L.Lock()
			if kv.config.Num == newConfig.Num {
				isNeedSend = true
				delShards = copyMap(kv.delShards)
				deepCopyViaGob(&kv.config, &configC)
			}
			i = kv.config.Num
			kv.applyCond.L.Unlock()

			if isNeedSend {
				kv.sendMoveIn(configC, delShards)
			}
		}

		time.Sleep(MonitorTimePeriod)
	}
}

func (kv *ShardKV) sendMoveIn(config shardctrler.Config, shards map[int]struct{}) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	for shard := range shards {
		var shardData []byte
		var clientLastRequest map[int64]Request
		for {
			kv.applyCond.L.Lock()
			if len(kv.shardProgressingRequests[shard]) == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.shardDB[shard])
				shardData = w.Bytes()
				clientLastRequest = kv.clientLastRequest
				kv.applyCond.L.Unlock()
				break
			}
			kv.applyCond.L.Unlock()
			time.Sleep(100 * time.Millisecond)
		}

		args := MoveInArgs{
			Num:               config.Num,
			Shard:             shard,
			Data:              shardData,
			ClientId:          int64(kv.gid),
			RequestId:         nrand(),
			ClientLastRequest: clientLastRequest,
		}

		for {
			if servers, ok := config.Groups[config.Shards[shard]]; ok {
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					var reply GetReply
					ok := srv.Call("ShardKV.MoveIn", &args, &reply)

					DPrintf("G%d S%d Call G%d MoveIn %t,reply: %+v", kv.gid, kv.me, config.Shards[shard], ok, reply)
					if _, isLeader := kv.rf.GetState(); !isLeader {
						return
					}

					if ok && reply.Err == OK {
						kv.sync(Op{
							Type:      "MoveOut",
							Shard:     shard,
							ClientId:  int64(kv.gid),
							RequestId: args.RequestId,
						})
						goto NextShard
					}
					if ok && (reply.Err == ErrWrongLeader) {
						continue
					}
					if ok && (reply.Err == ErrWrongGroup) {
						break
					}
				}
			}
			time.Sleep(MonitorTimePeriod)
		}
	NextShard:
	}
}

func genShards(currentGid int, newShards [shardctrler.NShards]int) map[int]struct{} {
	newShardsMap := map[int]struct{}{}

	for shard, gid := range newShards {
		if gid == currentGid {
			newShardsMap[shard] = struct{}{}
		}
	}

	return newShardsMap
}

func copyMap(m map[int]struct{}) map[int]struct{} {
	result := make(map[int]struct{})
	for key, val := range m {
		result[key] = val
	}
	return result
}

func deepCopyViaGob(src, dst interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(&buf).Decode(dst)
}

func removeInt64(s []int64, i int64) []int64 {
	for j, v := range s {
		if v == i {
			return append(s[:j], s[j+1:]...)
		}
	}
	return s
}
