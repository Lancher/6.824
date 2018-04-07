package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"log"
	"math/rand"
	"time"
)

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// TODO Your data here (2A, 2B, 2C).

	// states on all servers
	currentTerm int	// latest term server has seen
	votedFor int	// candidateId that received vote in current term
	log [] string	// log entries; each entry contains command for state machine

	// states on the leader
	nextIndex [] int	// index of highest log entry known to be committed
	matchIndex [] int	// index of highest log entry applied to state machine

	// states defined by me
	who int // follower, candidate, leader
	votes int
	fAppendEntriesTimeoutChan chan int
	cElectionTimeoutChan chan int
	lAppendEntriesTimeoutChan chan int
	fAppendEntriesTimeoutID int
	cElectionTimeoutID int
	lAppendEntriesTimeoutID int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// TODO Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.who == 2
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// TODO Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// TODO Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// AppendEntries
//
type AppendEntriesArgs struct {
	// TODO Your data here (2A, 2B).

	Term int	// leader’s term
	LeaderId int	// so follower can redirect clients
	PreLogIndex int	// index of log entry immediately preceding new ones
	PreLogTerm int	// term of prevLogIndex entry
	Entries [] string // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int	// leader’s commitIndex
}

type AppendEntriesReply struct {
	// TODO Your data here (2A).

	Term int		// currentTerm
	Success bool	// true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO Your code here (2A, 2B).

	rf.mu.Lock()
	if rf.currentTerm == args.Term {
		if rf.who == 0 {
			// start wait append entries
			rf.fAppendEntriesTimeoutID++
			go func(id int) {
				select {
				case <-time.After(time.Duration(200 + rand.Intn(200)) * time.Millisecond):
					rf.fAppendEntriesTimeoutChan <- id
				}
			}(rf.fAppendEntriesTimeoutID)
			// fill the reply
			reply.Term = rf.currentTerm
			reply.Success = true
		}

	} else if rf.currentTerm < args.Term {
		// become follower
		log.Println(rf.me, "become follower")
		rf.who = 0
		rf.votedFor = -1
		rf.votes = 0
		// update term
		rf.currentTerm = args.Term
		// start to wait append entries
		rf.fAppendEntriesTimeoutID++
		go func(id int) {
			select {
			case <-time.After(time.Duration(200 + rand.Intn(200)) * time.Millisecond):
				rf.fAppendEntriesTimeoutChan <- id
			}
		}(rf.fAppendEntriesTimeoutID)
		// fill the reply
		reply.Term = rf.currentTerm
		reply.Success = true
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	rf.mu.Unlock()
}

//
// RequestVote
//
type RequestVoteArgs struct {
	// TODO Your data here (2A, 2B).

	Term int	// candidate’s term
	CandidateId int	// candidate requesting vote
	LastLogIndex int	// index of candidate’s last log entry
	LastLogTerm int	// term of candidate’s last log entry
}

type RequestVoteReply struct {
	// TODO Your data here (2A).

	Term int	// currentTerm
	VoteGranted bool	// true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO Your code here (2A, 2B).

	rf.mu.Lock()
	if rf.currentTerm == args.Term {
		if rf.who == 0 && rf.votedFor == -1 {
			// vote someone
			rf.votedFor = args.CandidateId
			rf.votes = 0
			// start wait append entries
			rf.fAppendEntriesTimeoutID++
			go func(id int) {
				select {
				case <-time.After(time.Duration(200 + rand.Intn(200)) * time.Millisecond):
					rf.fAppendEntriesTimeoutChan <- id
				}
			}(rf.fAppendEntriesTimeoutID)
			// fill reply
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		} else {
			// fill reply
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	} else if rf.currentTerm < args.Term {
		// become follower
		log.Println(rf.me, "become follower")
		rf.who = 0
		rf.votedFor = args.CandidateId
		rf.votes = 1
		// update term
		rf.currentTerm = args.Term
		// start wait append entries
		rf.fAppendEntriesTimeoutID++
		go func(id int) {
			select {
			case <-time.After(time.Duration(200 + rand.Intn(200)) * time.Millisecond):
				rf.fAppendEntriesTimeoutChan <- id
			}
		}(rf.fAppendEntriesTimeoutID)
		// fill reply
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		// fill reply
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//



//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// TODO Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// TODO Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// TODO Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	log.Println("------------------------------------------------")
	log.Println(len(peers), me)
	log.Println("------------------------------------------------")

	// initialization
	rf.fAppendEntriesTimeoutChan = make(chan int, 100)
	rf.cElectionTimeoutChan = make(chan int, 100)
	rf.lAppendEntriesTimeoutChan = make(chan int, 100)

	// start to wait append entries
	rf.fAppendEntriesTimeoutID++
	go func(id int) {
		select {
		case <-time.After(time.Duration(200 + rand.Intn(200)) * time.Millisecond):
			rf.fAppendEntriesTimeoutChan <- id
		}
	}(rf.fAppendEntriesTimeoutID)

	// ioloop

	go func() {
		for {
			select {
			case id := <- rf.fAppendEntriesTimeoutChan:
				rf.mu.Lock()
				// is follower & not late timeout
				if rf.who == 0 && rf.fAppendEntriesTimeoutID == id {
					// become candidate
					log.Println(rf.me, "become candidate")
					rf.who = 1
					rf.votedFor = -1
					rf.votes = 0
					// increment terms
					rf.currentTerm++
					// vote myself
					rf.votedFor = rf.me
					rf.votes = 1
					// start election timeout
					rf.cElectionTimeoutID++
					go func(id int) {
						select {
						case <-time.After(time.Duration(200 + rand.Intn(200)) * time.Millisecond):
							rf.cElectionTimeoutChan <- id
						}
					}(rf.cElectionTimeoutID)
					// send request votes
					for server := 0; server < len(rf.peers); server++ {
						if server == rf.me { continue }
						go func(server int, term int, candidateId int, lastLogIndex int, lastLogTerm int) {
							args := RequestVoteArgs{
								Term: term,
								CandidateId: candidateId,
								LastLogIndex: lastLogIndex,
								LastLogTerm: lastLogTerm,
							}
							reply := RequestVoteReply{}
							ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
							if ok {
								rf.mu.Lock()
								// if candidate & not late reply
								if rf.who == 1 {
									if rf.currentTerm == term {
										if rf.currentTerm == reply.Term {
											if reply.VoteGranted {
												rf.votes++
												if len(rf.peers)/2 < rf.votes {
													// become leader
													log.Println(rf.me, "become leader")
													rf.who = 2
													rf.votedFor = -1
													rf.votes = 0
													// leader send append entries to all peers
													go func() {
														// send append entries
														rf.mu.Lock()
														rf.lAppendEntriesTimeoutID++
														id := rf.lAppendEntriesTimeoutID
														rf.mu.Unlock()
														rf.lAppendEntriesTimeoutChan <- id
													}()
												}
											}
										}
									} else if rf.currentTerm < reply.Term {
										// become follower
										log.Println(rf.me, "become follower")
										rf.who = 0
										rf.votedFor = -1
										rf.votes = 0
										rf.currentTerm = reply.Term
										// start wait append entries
										rf.fAppendEntriesTimeoutID++
										go func(id int) {
											select {
											case <-time.After(time.Duration(200+rand.Intn(200)) * time.Millisecond):
												rf.fAppendEntriesTimeoutChan <- id
											}
										}(rf.fAppendEntriesTimeoutID)
									} else {
										// this will not happen
									}
								}
								rf.mu.Unlock()
							}
						}(server, rf.currentTerm, rf.me, -1, -1)
					}
				}
				rf.mu.Unlock()
			case id := <- rf.cElectionTimeoutChan:
				// candidate -> candidate
				rf.mu.Lock()
				if rf.who == 1 && rf.cElectionTimeoutID == id {
					// become candidate
					rf.who = 1
					rf.votedFor = -1
					rf.votes = 0
					// increment terms
					rf.currentTerm++
					// vote myself
					rf.votedFor = rf.me
					rf.votes = 1
					// start election timeout
					rf.cElectionTimeoutID++
					go func(id int) {
						select {
						case <-time.After(time.Duration(200 + rand.Intn(200)) * time.Millisecond):
							rf.cElectionTimeoutChan <- id
						}
					}(rf.cElectionTimeoutID)
					// send request votes
					for server := 0; server < len(rf.peers); server++ {
						if server == rf.me { continue }
						go func(server int, term int, candidateId int, lastLogIndex int, lastLogTerm int) {
							args := RequestVoteArgs{
								Term: term,
								CandidateId: candidateId,
								LastLogIndex: lastLogIndex,
								LastLogTerm: lastLogTerm,
							}
							reply := RequestVoteReply{}
							ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
							if ok {
								rf.mu.Lock()
								// if candidate & not late reply
								if rf.who == 1 {
									if rf.currentTerm == term {
										if rf.currentTerm == reply.Term {
											if reply.VoteGranted {
												rf.votes++
												if len(rf.peers)/2 < rf.votes {
													// become leader
													log.Println(rf.me, "become leader")
													rf.who = 2
													rf.votedFor = -1
													rf.votes = 0
													// leader send append entries to all peers
													go func() {
														// send append entries
														rf.mu.Lock()
														rf.lAppendEntriesTimeoutID++
														id := rf.lAppendEntriesTimeoutID
														rf.mu.Unlock()
														rf.lAppendEntriesTimeoutChan <- id
													}()
												}
											}
										}
									} else if rf.currentTerm < reply.Term {
										// become follower
										log.Println(rf.me, "become follower")
										rf.who = 0
										rf.votedFor = -1
										rf.votes = 0
										rf.currentTerm = reply.Term
										// start wait append entries
										rf.fAppendEntriesTimeoutID++
										go func(id int) {
											select {
											case <-time.After(time.Duration(200+rand.Intn(200)) * time.Millisecond):
												rf.fAppendEntriesTimeoutChan <- id
											}
										}(rf.fAppendEntriesTimeoutID)
									} else {
										// this will not happen
									}
								}
								rf.mu.Unlock()
							}
						}(server, rf.currentTerm, rf.me, -1, -1)
					}
				}
				rf.mu.Unlock()
			case id := <- rf.lAppendEntriesTimeoutChan:
				rf.mu.Lock()
				// still leader & not late timeout
				if rf.who == 2 && rf.lAppendEntriesTimeoutID == id {
					// start new append entries timeout
					go func() {
						// wait 200 milli sec
						select {
						case <-time.After(time.Duration(150) * time.Millisecond):
						}
						// send append entries
						rf.mu.Lock()
						rf.lAppendEntriesTimeoutID++
						id := rf.lAppendEntriesTimeoutID
						rf.mu.Unlock()
						rf.lAppendEntriesTimeoutChan <- id
					}()
					// send append entries
					for server := 0; server < len(rf.peers); server++ {
						if server == rf.me { continue }
						go func(server int, term int) {
							args := AppendEntriesArgs{
								Term: term,
								LeaderId: rf.me,
								PreLogIndex: -1,
								PreLogTerm: -1,
								Entries: [] string{},
								LeaderCommit: -1,
							}
							reply := AppendEntriesReply{}
							ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
							if ok {
								rf.mu.Lock()
								if rf.who == 2 {
									if rf.currentTerm == reply.Term {
										// nothing to do
									} else if rf.currentTerm < reply.Term {
										// become follower
										log.Println(rf.me, "become follower")
										rf.who = 0
										rf.votedFor = -1
										rf.votes = 1
										rf.currentTerm = reply.Term
										// start wait append entries
										rf.fAppendEntriesTimeoutID++
										go func(id int) {
											select {
											case <-time.After(time.Duration(200+rand.Intn(200)) * time.Millisecond):
												rf.fAppendEntriesTimeoutChan <- id
											}
										}(rf.fAppendEntriesTimeoutID)
									} else {
										// this will not happen
									}
								}
								rf.mu.Unlock()
							}

						}(server, rf.currentTerm)
					}
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
