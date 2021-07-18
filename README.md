# raft
> Implementations of the Raft algorithm within a simulated network.

This is an endeavor to better understand the complex problem of distibuted consensus by implementing the Raft algorithm.
See [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) for details.

## Project Phases
### In-Memory Servers
This first phase will be to translate the fundamental Raft processes 
into _understandable_ and _testable_ blocks of code.
Before proceeding to the next phase, tests must be written to verify the implementation 
functions as documented in the [Raft specification](https://raft.github.io/raft.pdf).

According to the Raft specification, the following 5 properties must always be true:

1. Election Safety: at most one leader can be elected in a given term. §5.2
2. Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3
3. Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3
4. Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms. §5.4
5. State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index. §5.4.3 

### Containerized Nodes
After the in-memory implementation is complete and validated, in-memory server-like objects will be 
converted into individual Docker containers within a Kubernetes cluster. This will 
more closely simulate a real-world distributed system of shared-nothing servers for which Raft was designed.
