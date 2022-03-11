package surfstore

import (
	context "context"
	"fmt"
	// "fmt"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	serverId int64
	ipList		 []string
	ip 		 string
	isLeader bool
	term     int64
	log      []*UpdateOperation
	commitIndex int64
	pendingReplicas []chan bool
	lastApplied int64
	nextIndex []int64
	successfulReplication chan bool
	
	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if s.isCrashed{
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader{
		return nil, ERR_NOT_LEADER
	} else {
		majorityAlive := false
		for{ // assuming that if majority nodes dont return it blocks and keeps waiting here
			if majorityAlive{
				break
			}
			totalAlive := 1
			for id, addr := range s.ipList{
				if int64(id) == s.serverId{
					continue
				}
				func (){
					conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					client := NewRaftSurfstoreClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel() // Since term is -1 we want to always return false append entry if server is not crashed
					_, err := client.AppendEntries(ctx, &AppendEntryInput{Term: s.term, PrevLogIndex: -1, PrevLogTerm: -1, Entries: []*UpdateOperation{}, LeaderCommit: s.commitIndex})
					if err!= nil && strings.Contains(err.Error(), STR_SERVER_CRASHED){
						conn.Close()
						return
					}
					if err == nil{
						totalAlive = totalAlive + 1 
					}
				}()
				if totalAlive > len(s.ipList)/2{
					majorityAlive = true
					break
				}
			}	
		}
		return s.metaStore.GetFileInfoMap(ctx, empty)
	}
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	if s.isCrashed{
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader{
		return nil, ERR_NOT_LEADER
	} else {
		majorityAlive := false
		for{ // assuming that if majority nodes dont return it blocks and keeps waiting here
			if majorityAlive{
				break
			}
			totalAlive := 1
			for id, addr := range s.ipList{
				if int64(id) == s.serverId{
					continue
				}
				func (){
					// fmt.Println("entered internal function")
					conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					// fmt.Println("conn created")
					client := NewRaftSurfstoreClient(conn)
					// fmt.Println("client created")
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					// fmt.Println("ctc created")
					defer cancel() // TO DO: what log to send
					_, err := client.AppendEntries(ctx, &AppendEntryInput{Term: s.term, PrevLogIndex: -1, PrevLogTerm: -1, Entries: []*UpdateOperation{}, LeaderCommit: s.commitIndex})
					// fmt.Println("cleared appendEntries")
					if err!=nil && strings.Contains(err.Error(), STR_SERVER_CRASHED){
						conn.Close()
						return
					}
					if err == nil{
						totalAlive = totalAlive + 1 
					}
					// fmt.Println("exited internal function")
				}()
				if totalAlive > len(s.ipList)/2{
					majorityAlive = true
					break
				}
			}	
		}
		return s.metaStore.GetBlockStoreAddr(ctx, empty)
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// fmt.Println("Entered Update File")
	opr := UpdateOperation{
        Term: s.term,
        FileMetaData: filemeta,
    }
    s.log = append(s.log, &opr)
	replicated := make(chan bool)
	s.pendingReplicas = append(s.pendingReplicas, replicated)
	go s.AttemptReplication()

    success := <-replicated // even heartbeat can send value here
	if success { // block until successful
		// fmt.Println("Received success in replication")
		return s.metaStore.UpdateFile(ctx, filemeta)
    } 
	return nil, nil
}

func (s *RaftSurfstore) AttemptReplication(){ // returns true when replication equal to leaders log is done at majority of servers
	var targetIdx int64
	var heartbeat bool
	if int(s.commitIndex+1) > len(s.log)-1{
		heartbeat = true
		targetIdx = int64(len(s.log)-1)
	} else {
		heartbeat = false
		targetIdx = s.commitIndex+1
	}
	// fmt.Printf("Entered Attempt Replication with targetId %d\n", targetIdx)
	replicateChan := make(chan *AppendEntryOutput, len(s.ipList))
    for idx, _ := range s.ipList {
        if int64(idx) == s.serverId {
            continue
        }
        go s.Replicate(int64(idx), targetIdx, replicateChan, heartbeat)
    }
    replicateCount := 1
	nodesProbed := 1
	for{
		if nodesProbed >= len(s.ipList){
			break
		}
		rep := <-replicateChan
		if rep!=nil && rep.Success{
			// fmt.Println("successful replica")
			replicateCount++
			nodesProbed++
		} else { 
			// fmt.Println("successful probe")
			nodesProbed++ } // if crashed then also return to channel
		if replicateCount > len(s.ipList)/2{
			if(s.commitIndex < targetIdx){
				fmt.Printf("Reached Majority for %d and sending true. TargetId: %d, commitIndex: %d\n", s.serverId, targetIdx, s.commitIndex)
				if(len(s.pendingReplicas)>int(targetIdx)){
					s.pendingReplicas[targetIdx] <- true // no need to signal second time otherwise will block
				}
				s.commitIndex = targetIdx // dont break so that we can reuse for heartbeat; updatefile is already signalled	
			}
		}
	}
	// fmt.Println("Exiting Attempt Replication with targetId: ", targetIdx)
}
func (s *RaftSurfstore) Replicate(followerId, entryId int64, replicateChan chan *AppendEntryOutput, isHeartbeat bool) {
	for {
		// fmt.Printf("Entered Replicate with nextId %d and followerId %d\n", s.nextIndex[followerId], followerId)
	    addr := s.ipList[followerId]
        conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)
		var input *AppendEntryInput
		if s.nextIndex[followerId] == 0{ // either initial stage or reached here after decrementation
			if len(s.log) != 0{ // something has been updated to the server 
				input = &AppendEntryInput{ // can be treated as heartbeat without any entries; but for case when log is empty
					Term: s.term,
					PrevLogTerm: -1,
					PrevLogIndex: -1,
					Entries: s.log[s.nextIndex[followerId]:entryId+1],
					LeaderCommit: s.commitIndex,
				}
			}else {
				input = &AppendEntryInput{ // can be treated as heartbeat without any entries; but for case when log is empty
					Term: s.term,
					PrevLogTerm: -1,
					PrevLogIndex: -1,
					Entries: []*UpdateOperation{},
					LeaderCommit: s.commitIndex,
				}
			}
		} else {
			if int(s.nextIndex[followerId]) == len(s.log){
				input = &AppendEntryInput{
					Term: s.term,
					PrevLogTerm: s.log[s.nextIndex[followerId]-1].Term,
					PrevLogIndex: s.nextIndex[followerId]-1,
					Entries: []*UpdateOperation{}, // nothing to be sent
					LeaderCommit: s.commitIndex,
				}	
			} else {
				if s.nextIndex[followerId] <= entryId{
					input = &AppendEntryInput{
						Term: s.term,
						PrevLogTerm: s.log[s.nextIndex[followerId]-1].Term,
						PrevLogIndex: s.nextIndex[followerId]-1,
						Entries: s.log[s.nextIndex[followerId]:entryId+1],
						LeaderCommit: s.commitIndex,
					}		
				} else {
					input = &AppendEntryInput{
						Term: s.term,
						PrevLogTerm: s.log[s.nextIndex[followerId]-1].Term,
						PrevLogIndex: s.nextIndex[followerId]-1,
						Entries: []*UpdateOperation{},
						LeaderCommit: s.commitIndex,
					}
				}
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		if (output != nil) && (output.Term > s.term){
			s.term = output.Term
			s.isLeader = false
		}
		if output != nil && output.Success {
			s.nextIndex[followerId] = output.MatchedIndex+1
			replicateChan <- output
			conn.Close()
			return
		}
		if output != nil && !output.Success {
			s.nextIndex[followerId]--
			if s.nextIndex[followerId] < 0{
				s.nextIndex[followerId] = 0 // this basically means i need to send him everything 
				replicateChan <- &AppendEntryOutput{ServerId: -1, Term: s.term, Success: false, MatchedIndex: -1}
				conn.Close()
				return // added to handle crashed nodes
			}
		} 
		if err!= nil && !(strings.Contains(err.Error(), STR_SERVER_CRASHED)){
			replicateChan <- &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: false, MatchedIndex: -1}
			return // added to handle crashed nodes
		}
		if err != nil{
			conn.Close()
			return
		}
		conn.Close()
		cancel()
    }
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// fmt.Printf("Entered AppendEntries\n")
	output := &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: false, MatchedIndex: -1}
	falseResponse := false
	if s.isCrashed{
		// fmt.Println("AppendEntries: crashed server")
		return nil, ERR_SERVER_CRASHED
	}
	if (input.Term < s.term) { // TODO; revisit - is it safe to return -1
		// fmt.Println("AppendEntries: stale entry")
		return output, nil
	} // matchedIndex to be implemented
	if input.Term > s.term{
		s.isLeader = false
		s.term = input.Term
	}
	if (input.PrevLogTerm == -1) && (len(input.Entries)==0){ 
		falseResponse = true 
	}// if prevLogTerm is -1 no need to make below check because entire log needs to be replaced
	if input.PrevLogTerm != -1{
		// fmt.Println("AppendEntries: prev term not matching")
		if !(((len(s.log)-1) >= int(input.PrevLogIndex)) && (s.log[input.PrevLogIndex].Term == input.PrevLogTerm)){
			falseResponse = true	
		}	
	}
	// fmt.Println("entered append operation")
	if !falseResponse{
		s.log = append(s.log[:input.PrevLogIndex+1], input.Entries...)
		output.Success = true
	}
	// fmt.Println("exited append operation")
	// if input.LeaderCommit > s.commitIndex {
	s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	for s.lastApplied < s.commitIndex{ // no need to check for updateFile errors because leader would have commited only if there are no errors
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}
	// }
	// fmt.Printf("Exiting AppendEntries\n")
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// fmt.Printf("Entered SetLeader\n")
	if s.isCrashed {
		// fmt.Printf("Exiting SetLeader\n")
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	} else {
		s.term++
		s.isLeader = true
		for i:=0; i<len(s.ipList); i++{
			s.nextIndex[i] = int64(len(s.log))
		}
		// fmt.Printf("Exiting SetLeader\n")
		return &Success{Flag: true}, nil
	}
	
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// fmt.Printf("Entering SendHeartBeat\n")
	if s.isCrashed{
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	if !s.isLeader{
		return &Success{Flag: false}, nil
	}
	go s.AttemptReplication()
	// fmt.Printf("Exiting SendHeartBeat\n")
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
