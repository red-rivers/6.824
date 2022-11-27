// package shardkv

// func (kv *ShardKV) checkEntryInCurrentTermAction() {
// 	if !kv.rf.HasLogInCurrentTerm() {
// 		kv.Execute(NewEmptyEntryCommand(), &CommandResponse{})
// 	}
// }

// func (kv *ShardKV) applyEmptyEntry() *CommandResponse {
// 	return &CommandResponse{OK, ""}
// }