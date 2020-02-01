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

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"labrpc"
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

// Entry defines a log entry
type Entry struct {
	Term    int
	Command interface{}
}

type State int
const (
	FOLLOWER State = iota + 1
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // latest term server has seen (initialized to 0 on the first boot, increases monotonically)
	votedFor int // candidate that received vote in current term (or null if none)
	log []*Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increase monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increase monotonically)

	nextIndex []int // for each server, index of the next log entry to send to that server
	matchIndex []int // index of the highest log entry known to be replicated on that server (initialized to 0, increase monotonically)

	State State
	isLeader bool // is this peer a leader

	receiveHeartbeat bool // Raft received heartbeat or not
	applyCh chan ApplyMsg // simulator of the network
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.isLeader
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // candidate's term
	CandidateID int // candidate requesting vote
	LastLogIndex int // index of the candidate's last log entry
	LastLogTerm int // term of the candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term int // leader's term
	LeaderId int // so follower can redirect clients
	PrevLogIndex int // index of log entries immediately proceeding the new one
	PrevLogTerm int // term of prevLogIndex entry
	Entries []*Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader's commit index
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Println("receive request vote from ",  args.CandidateID, args.Term, " to ", rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.State = FOLLOWER
		rf.votedFor = -1
	}

	// if vote for another candidate
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	if args.LastLogIndex == -1 {
		if len(rf.log) == 0 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			fmt.Println(rf.me, " granted vote for ", args.CandidateID)
			return
		}
	}

	// check log up-to-date
	if args.LastLogIndex < len(rf.log) || (rf.log[len(rf.log)-1].Term > args.LastLogTerm) {
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	fmt.Println(rf.me, " granted vote for ", args.CandidateID)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else {
		rf.currentTerm = args.Term
		// heartbeat
		if len(args.Entries) == 0 {
			if args.Term > rf.currentTerm {
				rf.isLeader = false
			}

			rf.receiveHeartbeat = true
			if !rf.isLeader {
				rf.State = FOLLOWER
			}

			return
		}
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if args.PrevLogIndex >= len(rf.log) {
			return
		}

		rf.log = rf.log[:args.PrevLogIndex]
	}

	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	if !rf.isLeader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, &Entry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.mu.Unlock()

	wg := sync.WaitGroup{}
	for i := range rf.peers {
		wg.Add(1)

		go func(i int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			success := false
			for !success {
				if len(rf.log) > rf.nextIndex[i] {
					prevLogIndex := rf.nextIndex[i] - 1
					prevLogTerm := 0
					if prevLogIndex != -1 {
						prevLogTerm = rf.log[prevLogIndex].Term
					}

					appendMsg := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      rf.log[rf.nextIndex[i]:],
						LeaderCommit: rf.commitIndex,
					}

					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(i, appendMsg, reply)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
					}

					if !reply.Success {
						rf.nextIndex[i]--
					} else {
						rf.nextIndex[i]++
						rf.matchIndex[i]++
						success = true
					}
				}
			}

			wg.Done()
		}(i)
	}

	wg.Wait()

	rf.mu.Lock()
	term := rf.currentTerm
	for N := len(rf.log); N > rf.commitIndex; N = N-1 {
		count := 0
		for _, match := range rf.matchIndex {
			if match >= N {
				count++
			}
		}

		if count > len(rf.peers) / 2 && N < len(rf.log) && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				// only send to client in leader
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
			}

			break
		}
	}

	index := rf.commitIndex
	rf.mu.Unlock()

	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	fmt.Println("start election", rf.me)
	rf.currentTerm++
	rf.State = CANDIDATE
	rf.votedFor = rf.me
	term := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	rf.mu.Unlock()

	requestVoteCount := 0
	countLock := sync.Mutex{}
	wg := sync.WaitGroup{}
	for i := range rf.peers {
		wg.Add(1)

		go func(i int, term int, lastLogIndex int, lastLogTerm int) {
			args := &RequestVoteArgs{
				Term: term,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			}

			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, args, reply); ok {
				if reply.VoteGranted {
					fmt.Println(rf.me, " got one vote in term ", reply.Term, " from ", i)
					countLock.Lock()
					requestVoteCount++
					countLock.Unlock()
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.State = FOLLOWER
						rf.isLeader = false
					}

					rf.mu.Unlock()
				}
			} else {
				fmt.Println(rf.me, " cannot connect ", i, " during election in term ", rf.currentTerm)
			}

			wg.Done()
		}(i, term, lastLogIndex, lastLogTerm)
	}

	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if there is a majority of votes
	if requestVoteCount > len(rf.log) / 2 {
		fmt.Println(rf.me, " became leader term", rf.currentTerm)
		rf.State = LEADER
		rf.isLeader = true
	} else {
		rf.State = FOLLOWER
	}
}

func (rf *Raft) sendHeartbeat() {
	for ;; {
		rf.mu.Lock()
		prevLogIndex := len(rf.log)-1
		prevLogTerm := 0
		if prevLogIndex >= 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}

		heartbeatMsg := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		wg := sync.WaitGroup{}
		for i := range rf.peers {
			wg.Add(1)

			go func(i int) {
				fmt.Println(rf.me, " sends heartbeat to ", i)
				rf.sendAppendEntries(i, heartbeatMsg, &AppendEntriesReply{})
				wg.Done()
			}(i)
		}

		wg.Wait()
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) listenHeartbeatForElection() {
	for ;; {
		rf.mu.Lock()
		if rf.isLeader {
			rf.mu.Unlock()
			continue
		}

		rf.receiveHeartbeat = false
		rf.mu.Unlock()

		timeout := rand.Intn(150) + 450
		time.Sleep(time.Duration(timeout) * time.Millisecond)

		rf.mu.Lock()
		if !rf.receiveHeartbeat {
			rf.mu.Unlock()
			rf.startElection()
			fmt.Println(rf.me, " finishes election ", rf.isLeader)
			rf.mu.Lock()
			if rf.isLeader {
				rf.mu.Unlock()
				fmt.Println(rf.me, " became leader and starts to send heartbeats")
				go rf.sendHeartbeat()
			}

			continue
		}

		rf.mu.Unlock()
	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		currentTerm: 1,
		votedFor:    -1,
		log:         make([]*Entry, 0),
		commitIndex: -1,
		lastApplied: -1,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		State:       FOLLOWER,
		isLeader:    false,
		applyCh:     applyCh,
	}

	for i := range peers {
		rf.matchIndex[i] = -1
	}

	go rf.listenHeartbeatForElection()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
