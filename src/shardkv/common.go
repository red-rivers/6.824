package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutDated    = "ErrOutDated"
	ErrNotReady    = "ErrNotReady"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type Err string

type CommandRequest struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientId  int64
	CommandId int64
}

type CommandResponse struct {
	Err   Err
	Value string
}

type ShardOperationRequest struct {
	ConfigNum int
	ShardIDs  []int
}

type ShardOperationResponse struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]OperationContext
}

type OperationContext struct {
	MaxAppliedCommandId    int64
	LastResponse *CommandResponse
}

func (op *OperationContext) deepCopy () OperationContext {
	return OperationContext{
		MaxAppliedCommandId: op.MaxAppliedCommandId,
		LastResponse: &CommandResponse{
			Err: op.LastResponse.Err,
			Value: op.LastResponse.Value,	
		},
	}
}