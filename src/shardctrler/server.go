package shardctrler

import (
	"bytes"
	"log"
	"sort"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	Debug = false
)

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
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs           []Config // indexed by config num
	applyCond         *sync.Cond
	lastAppliedIndex  int               // last applied index
	lastAppliedTerm   int               // last applied term
	clientLastRequest map[int64]Request // client last requests
}

type Request struct {
	RequestId int64
	Config    Config
}

type Op struct {
	// Your data here.
	Type      string
	Args      interface{}
	ClientId  int64
	RequestId int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.applyCond.L.Lock()
	req, ok := sc.clientLastRequest[args.ClientId]
	sc.applyCond.L.Unlock()

	if ok && req.RequestId == args.RequestId {
		reply.Err = OK
		return
	}

	op := Op{
		Type:      "Join",
		Args:      *args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	success := sc.waitApply(index, term)
	if !success {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.applyCond.L.Lock()
	req, ok := sc.clientLastRequest[args.ClientId]
	sc.applyCond.L.Unlock()

	if ok && req.RequestId == args.RequestId {
		reply.Err = OK
		return
	}

	op := Op{
		Type:      "Leave",
		Args:      *args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	success := sc.waitApply(index, term)
	if !success {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.applyCond.L.Lock()
	req, ok := sc.clientLastRequest[args.ClientId]
	sc.applyCond.L.Unlock()

	if ok && req.RequestId == args.RequestId {
		reply.Err = OK
		return
	}

	op := Op{
		Type:      "Move",
		Args:      *args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	success := sc.waitApply(index, term)
	if !success {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.applyCond.L.Lock()
	req, ok := sc.clientLastRequest[args.ClientId]
	sc.applyCond.L.Unlock()

	if ok && req.RequestId == args.RequestId {
		reply.Err = OK
		return
	}

	op := Op{
		Type:      "Query",
		Args:      *args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	success := sc.waitApply(index, term)
	if !success {
		reply.WrongLeader = true
		return
	}

	sc.applyCond.L.Lock()
	defer sc.applyCond.L.Unlock()

	reply.Err = OK
	if args.Num == -1 || args.Num >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
}

func (sc *ShardCtrler) waitApply(index int, term int) bool {
	sc.applyCond.L.Lock()
	defer sc.applyCond.L.Unlock()

	for sc.lastAppliedIndex < index {
		sc.applyCond.Wait()
	}

	return sc.lastAppliedIndex >= index && sc.lastAppliedTerm == term
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCond = sync.NewCond(&sc.mu)
	sc.lastAppliedIndex = 0
	sc.lastAppliedTerm = 0
	sc.clientLastRequest = make(map[int64]Request)

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.readPersist(persister.ReadSnapshot())
	go sc.applyListener()

	return sc
}

func (sc *ShardCtrler) applyListener() {
	for msg := range sc.applyCh {
		if !msg.CommandValid && msg.SnapshotValid {
			sc.readPersist(msg.Snapshot)
		}

		sc.applyCond.L.Lock()
		if msg.CommandValid && msg.CommandIndex > sc.lastAppliedIndex {
			// DPrintf("S%d receive apply msg: %v", sc.me, msg)
			op := msg.Command.(Op)
			req, ok := sc.clientLastRequest[op.ClientId]
			if !(ok && req.RequestId == op.RequestId) {
				request := Request{
					RequestId: op.RequestId,
					Config:    Config{},
				}
				switch op.Type {
				case "Join":
					args := op.Args.(JoinArgs)
					latestConfig := sc.configs[len(sc.configs)-1]
					newConfig := Config{}
					newConfig.Num = latestConfig.Num + 1
					newConfig.Shards = latestConfig.Shards
					newConfig.Groups = make(map[int][]string)
					for gid, servers := range latestConfig.Groups {
						newConfig.Groups[gid] = append([]string{}, servers...)
					}
					for gid, svrs := range args.Servers {
						newConfig.Groups[gid] = svrs
					}

					rebalanceShards(&newConfig)

					sc.configs = append(sc.configs, newConfig)
				case "Leave":
					args := op.Args.(LeaveArgs)
					latestConfig := sc.configs[len(sc.configs)-1]
					newConfig := Config{}
					newConfig.Num = latestConfig.Num + 1
					newConfig.Shards = latestConfig.Shards
					newConfig.Groups = make(map[int][]string)
					for gid, servers := range latestConfig.Groups {
						newConfig.Groups[gid] = append([]string{}, servers...)
					}
					for _, gid := range args.GIDs {
						delete(newConfig.Groups, gid)
					}

					rebalanceShards(&newConfig)

					sc.configs = append(sc.configs, newConfig)
				case "Move":
					args := op.Args.(MoveArgs)
					latestConfig := sc.configs[len(sc.configs)-1]
					newConfig := Config{}
					newConfig.Num = latestConfig.Num + 1
					newConfig.Shards = latestConfig.Shards
					newConfig.Shards[args.Shard] = args.GID
					newConfig.Groups = make(map[int][]string)
					for gid, servers := range latestConfig.Groups {
						newConfig.Groups[gid] = append([]string{}, servers...)
					}

					sc.configs = append(sc.configs, newConfig)
				case "Query":
					args := op.Args.(QueryArgs)
					if args.Num == -1 || args.Num >= len(sc.configs) {
						request.Config = sc.configs[len(sc.configs)-1]
					} else {
						request.Config = sc.configs[args.Num]
					}
				}
				sc.clientLastRequest[op.ClientId] = request
			}

			sc.lastAppliedIndex = msg.CommandIndex
			sc.lastAppliedTerm = msg.CommandTerm
			sc.applyCond.Broadcast()
			// DPrintf("S%d configs: %v", sc.me, sc.configs)
		}
		sc.applyCond.L.Unlock()

		sc.persist()
	}
}

func (sc *ShardCtrler) persist() {
	sc.applyCond.L.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.lastAppliedIndex)
	e.Encode(sc.lastAppliedTerm)
	e.Encode(sc.clientLastRequest)
	snapshot := w.Bytes()
	sc.rf.Snapshot(sc.lastAppliedIndex, snapshot)
	sc.applyCond.L.Unlock()
}

func (sc *ShardCtrler) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	sc.applyCond.L.Lock()
	defer sc.applyCond.L.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var configs []Config
	var lastAppliedIndex int
	var lastAppliedTerm int
	var clientLastRequest map[int64]Request
	if d.Decode(&configs) != nil ||
		d.Decode(&lastAppliedIndex) != nil ||
		d.Decode(&lastAppliedTerm) != nil ||
		d.Decode(&clientLastRequest) != nil {
		DPrintf("S%d read persist ERROR", sc.me)
		return
	} else {
		sc.configs = configs
		sc.lastAppliedIndex = lastAppliedIndex
		sc.lastAppliedTerm = lastAppliedTerm
		sc.clientLastRequest = clientLastRequest
	}
	DPrintf("S%d read persist LAI %d LAT %d Configs %v",
		sc.me, sc.lastAppliedIndex, sc.lastAppliedTerm, sc.configs)
}

func rebalanceShards(config *Config) {
	numGroups := len(config.Groups)
	if numGroups == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	shardsPerGroup := NShards / numGroups
	extraShards := NShards % numGroups

	targetShardsPerGroup := make(map[int]int)
	for _, gid := range getSortedKeys1(config.Groups) {
		targetShardsPerGroup[gid] = shardsPerGroup
		if extraShards > 0 {
			targetShardsPerGroup[gid]++
			extraShards--
		}
	}

	currentShardCounts := make(map[int]int)
	for _, gid := range config.Shards {
		if gid != 0 {
			currentShardCounts[gid]++
		}
	}

	freeGidShards := make(map[int]int)
	for _, gid := range getSortedKeys2(targetShardsPerGroup) {
		counts := targetShardsPerGroup[gid]
		if counts < currentShardCounts[gid] {
			freeGidShards[gid] = currentShardCounts[gid] - counts
		}
	}
	for _, gid := range getSortedKeys2(currentShardCounts) {
		counts := currentShardCounts[gid]
		if targetShardsPerGroup[gid] < counts {
			freeGidShards[gid] = counts - targetShardsPerGroup[gid]
		}
	}

	for _, gid := range getSortedKeys2(targetShardsPerGroup) {
		counts := targetShardsPerGroup[gid]
		if counts > currentShardCounts[gid] {
			for i := 0; i < counts-currentShardCounts[gid]; i++ {
				for shard, _gid := range config.Shards {
					if _gid == 0 || freeGidShards[_gid] > 0 {
						config.Shards[shard] = gid
						freeGidShards[_gid]--
						break
					}
				}
			}
		}
	}
}

func getSortedKeys1(m map[int][]string) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

func getSortedKeys2(m map[int]int) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// func SortedKeys[K comparable, V any](m map[K]V) []K {
// 	keys := make([]K, 0, len(m))
// 	for k := range m {
// 		keys = append(keys, k)
// 	}

// 	sort.Slice(keys, func(i, j int) bool {
// 		return fmt.Sprintf("%v", keys[i]) < fmt.Sprintf("%v", keys[j])
// 	})
// 	return keys
// }
