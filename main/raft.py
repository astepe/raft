import concurrent.futures as cf
import logging
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import wraps
from threading import Thread
from typing import Any, List, Tuple
from collections import namedtuple

Command = namedtuple("Command", "key value")

# logging.basicConfig(filename="./raftLogs.log", filemode="a", level=logging.INFO)
logging.basicConfig(level=logging.INFO)


class RaftServerState(Enum):
    follower = "follower"
    candidate = "candidate"
    leader = "leader"


def network_delay(func):
    """
    Simulates network delay by waiting
    a random length of time before
    executing the decorated function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        time.sleep(random.uniform(0.05, 0.075))
        return func(*args, **kwargs)

    return wrapper


class Entry:
    def __init__(self, command: Command, term: int, index: int):
        """
        Args:
            command (Any): The command to be stored
            term (int): Term when entry was received by leader
            index (int): Index when entry was received by leader
        """
        self._command = command
        self._term = term
        self._committed = False
        self._index = index

    @property
    def command(self):
        return self._command

    @property
    def term(self):
        return self._term

    @property
    def committed(self):
        return self._committed

    @property
    def index(self):
        return self._index

    def commit(self):
        self._committed = True

    @classmethod
    def from_entry(cls, entry: "Entry") -> "Entry":
        return Entry(command=entry.command, term=entry.term, index=entry.index)

    def __repr__(self):
        return f"Entry: command: {self.command}, term: {self.term}"


class RaftServer:
    def __init__(self, server_count: int):
        self._state: RaftServerState = RaftServerState.follower
        self._leader_id = None
        self._current_term = 0
        self._voted_for = None
        self._server_count = server_count
        self._commit_index = 0
        self._next_indexes = dict()
        self._last_applied = 0
        self._servers = dict()
        self._is_led = False
        self._election_timeout = random.uniform(0.15, 0.3)
        self._log: List[Entry] = list()
        self._check_thread = Thread(target=self._check_state)
        self._request_thread_pool = ThreadPoolExecutor(max_workers=server_count)
        self._state_machine = dict()
        self._id = uuid.uuid4()

    @property
    def id(self):
        """
        The unique id of the server
        """
        return self._id

    def start(self):
        self._running = True
        self._check_thread.start()

    def stop(self):
        self._check_thread.stop()
        self._running = False

    @property
    def leader_id(self):
        if self.state == RaftServerState.leader:
            return self.id
        return self._leader_id

    @leader_id.setter
    def leader_id(self, leader_id):
        if self.state != RaftServerState.leader:
            self._leader_id = leader_id

    @property
    def other_servers(self) -> List["RaftServer"]:
        if not hasattr(self, "_other_servers"):
            self._other_servers = {
                server.id: server
                for server in self.servers.values()
                if server.id != self.id
            }
        return self._other_servers

    @property
    def servers(self) -> List["RaftServer"]:
        return self._servers

    @servers.setter
    def servers(self, servers: List["RaftServer"]):
        self._servers = {server.id: server for server in servers}

    def _check_state(self):
        while self._running:
            if self.state == RaftServerState.follower:
                self._run_follower()

            self.state = RaftServerState.candidate
            self._run_candidate()

            if self.state == RaftServerState.leader:
                self._run_leader()

    def _run_follower(self):
        """
        Runs the follower routine. Waits for the election timeout
        in constant intervals. If the follower is not led before the
        timeout expires, the routine exits.
        """
        while self.state == RaftServerState.follower and self._is_led:
            logging.debug(f"Follower: {self.id} is being led")
            self._is_led = False
            time.sleep(self._election_timeout)

    def _run_candidate(self):
        """
        Runs the candidate routine. Attempts to gather
        a majority of votes from other nodes in order
        to become the new leader.
        """
        self._current_term += 1
        futures = [
            self._request_thread_pool.submit(
                server.request_vote,
                term=self._current_term,
                candidate_id=self.id,
                last_log_index=len(self._log) - 1 if self._log else 0,
                last_log_term=self._log[-1].term if self._log else 0,
            )
            for server in self.other_servers.values()
        ]
        self._voted_for = self.id
        results = [future.result() for future in cf.as_completed(futures)]
        vote_total = sum([result[1] for result in results]) + 1
        max_term = max([result[0] for result in results])

        if max_term > self._current_term:
            self._current_term = max_term
            self.state = RaftServerState.follower
        elif vote_total > self._server_count / 2:
            logging.info(
                f"Candidate: {self.id} elected as leader with {vote_total} votes..."
            )
            self.state = RaftServerState.leader
        else:
            logging.info(
                f"Candidate: {self.id} lost election with {vote_total} votes..."
            )
            self.state = RaftServerState.follower

    def _run_leader(self):
        """
        Runs the leader routine. Sends heartbeats to all other nodes
        to maintain leader status.
        """
        self._next_indexes = {
            server.id: len(self._log) for server in self.other_servers.values()
        }
        self._match_indexes = {server.id: 0 for server in self.other_servers.values()}
        while self.state == RaftServerState.leader:
            futures = [
                self._request_thread_pool.submit(
                    server._append_entries,
                    term=self._current_term,
                    leader_id=self.id,
                    prev_log_index=None,
                    prev_log_term=None,
                    entries=[],
                    leader_commit=self._commit_index,
                )
                for server in self.other_servers.values()
            ]
            results = [future.result() for future in cf.as_completed(futures)]
            time.sleep(0.05)
            logging.info(f"Leader: {self.id} heartbeat {results}")
        logging.info(f"Server: {self.id} no longer leader")

    def set_entries(self, commands: List[Command]) -> None:
        """
        Given a list of Entry objects, find the leader
        node and append the entries to its log, then issue
        AppendEntries calls in parallel to all follower servers
        to replicate the entries.

        Args:
            entries (List[Entry]): A list of Entry objects
                    to be replicated to all servers and eventually
                    committed to all state machines.
        """
        if self.state != RaftServerState.leader:
            leader = self._servers[self.leader_id]
            leader.set_entries(commands)
        else:

            entries = self._create_entries(commands)
            prev_log_index = len(self._log) - 1
            prev_log_term = self._log[-1].term if self._log else 0
            self._log.extend(entries)
            new_commit_index = len(self._log) - 1
            future = self._request_thread_pool.submit(
                self._replicate,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                new_commit_index=new_commit_index,
            )
            cf.wait([future])
            result = self._apply_entries(new_commit_index)

            return result

    def _replicate(self, prev_log_index, prev_log_term, new_commit_index):
        """
        Replicate logs on a majority of servers before returning. Replication
        will continue to process in the background after returning.

        Args:
            prev_log_index (int): The previous log index
            prev_log_term (int): The previous entry term
            new_commit_index (int): The new commit index which will be set after
                                etries are replicated on a majority of followers
        """

        successful_replications = 0

        def call_append_entries_with_retries(server, prev_log_index, prev_log_term):
            success = False

            while not success:
                follower_term, success = server._append_entries(
                    term=self._current_term,
                    leader_id=self.id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=self._log[prev_log_index + 1 :],
                    leader_commit=self._commit_index,
                )
                if follower_term > self._current_term:
                    self._current_term = follower_term
                    self._state = RaftServerState.follower
                    return

                if not success:
                    self._next_indexes[server.id] -= 1
                    prev_log_index = self._next_indexes[server.id]
                    prev_log_term = self._log[prev_log_index].term
                else:
                    self._next_indexes[server.id] = self._commit_index + 1
                    self._match_indexes[server.id] = self._commit_index

            nonlocal successful_replications
            successful_replications += 1

        for server in self.other_servers.values():
            self._request_thread_pool.submit(
                call_append_entries_with_retries, server, prev_log_index, prev_log_term
            )

        # replicated onto a majority of servers
        # If followers crash or run slowly, or if network packets are lost,
        # the leader retries AppendEntries RPCs indefinitely (even after
        # it has responded to the client) until all followers eventually
        # store all log entries.

        while successful_replications < self._server_count // 2:
            time.sleep(0.1)
        else:
            # A log entry is committed once the leader that created the
            # entry has replicated it on a majority of the servers
            self._commit_index = new_commit_index

    def _create_entries(self, commands: List[Command]) -> List[Entry]:
        """
        Given a list of Command objects, create Entry objects
        for each and return the resulting list.

        Args:
            commands (List[Command]): The commands to store in Entry objects

        Returns:
            List[Entry]: The Entry objects
        """
        entries = list()
        next_index = len(self._log)
        leader = self._servers[self.leader_id]
        for command in commands:
            entries.append(
                Entry(
                    command=command,
                    term=leader.current_term,
                    index=next_index,
                )
            )
            next_index += 1

        logging.info(f"Server {self.id} entries {self._log}")
        return entries

    def _apply_entries(self, index: int) -> None:
        for i, entry in enumerate(self._log):
            if i <= index:
                logging.info(f"Server {self.id} applying entry {entry}")
                self._state_machine[entry.command.key] = entry.command.value
                self._last_applied = i
            else:
                break

        return self._state_machine[entry.command.key]

    @network_delay
    def _append_entries(
        self,
        term: int,
        leader_id: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[Entry],
        leader_commit: int,
    ) -> Tuple:
        """
        Invoked by leader to replicate log entries

        Args:
            term (int): leader's term
            leader_id (int): So that followers can redirect clients
            prev_log_index (int): index of log entry immediately preceding new ones
            prev_log_term (int): term of prev_log_index entry
            entries (list): Log entries to store (empty for heartbeat, may send more than one for efficiency)
            leader_commit (int): leader's commit index

        Returns:
            Tuple: `current_term`, for the leader to update itself
                   `success`, True if follower contained entry matching prevLogIndex and prevLogTerm
        """
        logging.debug(
            f"Candidate: {self.id} received heartbeat for "
            f"term {term} and current term {self._current_term}"
        )
        self.state == RaftServerState.follower
        self._is_led = True
        self.leader_id = leader_id
        if term < self._current_term:
            return self._current_term, False

        # Reply false if log doesn’t contain an entry at
        # prev_log_index whose term matches prev_log_term
        if prev_log_index != -1:
            if prev_log_index in range(len(self._log)):
                if self._log[prev_log_index].term != prev_log_term:
                    return self._current_term, False
            else:
                return self._current_term, False

        # create new copies of all entries so objects in memory are not
        # shared between nodes
        copied_entries = list()
        for entry in entries:
            copied_entries.append(Entry.from_entry(entry))

        # If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it
        if prev_log_index + 1 in range(len(self._log)):
            for i in range(prev_log_index + 1, len(self._log)):
                if self._log[i].term != copied_entries[i - prev_log_index].term:
                    self._log = self._log[:i]
                    copied_entries = copied_entries[i:]
                    break

        # Append any new entries not already in the log
        logging.info(f"Follower: {self.id} replicating entries to log {copied_entries}")
        self._log.extend(copied_entries)

        if leader_commit > self._commit_index:
            self._commit_index = min(leader_commit, len(self._log) - 1)

        if self._commit_index > self._last_applied:
            self._apply_entries(self._commit_index)

        return self._current_term, True

    @network_delay
    def request_vote(
        self, term: int, candidate_id: int, last_log_index: int, last_log_term: int
    ) -> Tuple:
        """
        Invoked by candidates to gather votes

        Args:
            term (int): candidate's term
            candidate_id (int): candidate requesting vote
            last_log_index (int): index of candidate's last log entry
            last_log_term (int): term of candidate's last log entry

        Returns:
            Tuple: current_term, for candidate to update itself and `vote_granted`, true means candidate received vote
        """
        # Reply false if term < currentTerm
        if term < self._current_term:
            return self._current_term, False

        # If votedFor is null or candidateId, and candidate’s log is at
        # least as up-to-date as receiver’s log, grant vote
        if (self._voted_for is None or self._voted_for == candidate_id) and len(
            self._log
        ) <= last_log_index:
            self._voted_for = candidate_id
            return self._current_term, True

        return self._current_term, False

    @property
    def current_term(self):
        return self._current_term

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state: RaftServerState):
        self._state = state

    @property
    def leader(self):
        return self._leader

    @leader.setter
    def leader(self, leader: "RaftServer"):
        self._leader = leader

    def __repr__(self):
        return f"{self.__class__}, RaftServerState: {self.state}, id: {self.id}"


class RaftCluster:
    def __init__(self, server_count=3):
        self._servers = [RaftServer(server_count) for _ in range(server_count)]
        for server in self._servers:
            server.servers = list(self._servers)

    def start(self):
        for server in self._servers:
            server.start()
        # wait for leader to be found
        time.sleep(3)

    def set(self, **kwargs):
        commands = [Command(key=k, value=v) for k, v in kwargs.items()]
        self._servers[0].set_entries(commands)


if __name__ == "__main__":
    raft_cluster = RaftCluster()
    raft_cluster.start()
    raft_cluster.set(x=3, y=5)

    while True:
        time.sleep(0.1)
