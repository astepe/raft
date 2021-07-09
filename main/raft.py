import concurrent.futures as cf
import logging
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import wraps
from threading import Thread
from typing import Any, List, Tuple
from collections import namedtuple

Command = namedtuple("Command", "key value")

logging.basicConfig(filename="./raftLogs.log", filemode="a", level=logging.INFO)


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
        self._other_servers = dict()
        self._is_led = False
        self._election_timeout = random.uniform(0.15, 0.3)
        self._log: List[Entry] = list()
        self._check_thread = Thread(target=self._check_state)
        self._request_thread_pool = ThreadPoolExecutor(max_workers=server_count)
        self._state_machine = dict()

    def start(self):
        self._running = True
        self._check_thread.start()

    def stop(self):
        self._check_thread.stop()
        self._running = False

    @property
    def other_servers(self) -> List["RaftServer"]:
        return self._other_servers

    @other_servers.setter
    def other_servers(self, other_servers: List["RaftServer"]):
        self._other_servers = {id(server): server for server in other_servers}

    def _check_state(self):
        while self._running:
            if self.state == RaftServerState.follower:
                self._run_follower()

            self.state = RaftServerState.candidate
            self._run_candidate()

            if self.state == RaftServerState.leader:
                self._run_leader()

    def _run_follower(self):
        time.sleep(self._election_timeout)
        while self.state == RaftServerState.follower and self._is_led:
            self._is_led = False
            logging.debug(f"Follower: {id(self)} is being led")
            time.sleep(random.uniform(0.15, 0.3))

    def _run_candidate(self):

        self._current_term += 1
        futures = [
            self._request_thread_pool.submit(
                server.request_vote,
                term=self._current_term,
                candidate_id=id(self),
                last_log_index=len(self._log) - 1 if self._log else 0,
                last_log_term=self._log[-1].term if self._log else 0,
            )
            for server in self._other_servers
        ]
        results = [future.result() for future in cf.as_completed(futures)]
        vote_total = sum([result[1] for result in results])
        max_term = max([result[0] for result in results])

        if max_term > self._current_term:
            self.state = RaftServerState.follower
        elif vote_total > self._server_count / 2:
            logging.info(
                f"Candidate: {id(self)} elected as leader with {vote_total} votes..."
            )
            self.state = RaftServerState.leader
        else:
            logging.info(
                f"Candidate: {id(self)} lost election with {vote_total} votes..."
            )
            self.state = RaftServerState.follower

    def _run_leader(self):
        self._next_indexes = {
            id(server): len(self._log) for server in self._other_servers
        }
        while self.state == RaftServerState.leader:
            futures = [
                self._request_thread_pool.submit(
                    server._append_entries,
                    term=self._current_term,
                    leader_id=id(self),
                    prev_log_index=None,
                    prev_log_term=None,
                    entries=[],
                    leader_commit=self._commit_index,
                )
                for server in self._other_servers
            ]
            results = [future.result() for future in cf.as_completed(futures)]
            time.sleep(0.05)
            logging.info(f"Leader: {id(self)} heartbeat {results}")

    @property
    def command(self):
        for i in range(0, len(self._log), -1):
            if self._log[i].committed:
                return self._log[i]

    @command.setter
    def command(self, command: Any = None):
        """
        Used by leaders for replicating
        log entries and also as a heartbeat
        (a signal to check if a server is up or not
        — it doesn’t contain any log entries)
        """
        if self.state == RaftServerState.leader:
            self._log.append(Entry(command=command, term=self._current_term))
            failed_replications = 0
            for server in self._other_servers:
                failed_replications += server.receive_entry(command)
            if self._server_count / 2 < failed_replications:
                self._commit()
            for server in self._other_servers:
                server.commit()
        return 0

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
            leader = self._other_servers[self._leader_id]
            leader.set_entries(commands)
        else:

            entries = self._create_entries(commands)
            prev_log_index = len(self._log) - 1
            prev_log_term = self._log[-1].term
            self._log.extend(entries)
            self._request_thread_pool.submit(
                self._replicate,
                entries=entries,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                new_commit_index=len(self._log) - 1
            )

            return self._state_machine[entry.command.key]

    def _replicate(self, entries: List[Entry], prev_log_index, prev_log_term, new_commit_index):
        
        successful_replications = 0
        def increment_success(future):
            follower_term, success = future.result()

            nonlocal successful_replications
            if success:
                successful_replications += 1

        futures = [
            self._request_thread_pool.submit(
                server._append_entries,
                term=self._current_term,
                leader_id=id(self),
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self._commit_index,
            )
            for server in self._other_servers
        ]
        for future in futures:
            future.add_done_callback(increment_success)
        # replicated onto a majority of servers
        # If followers crash or run slowly, or if network packets are lost,
        # the leader retries AppendEntries RPCs indefinitely (even after
        # it has responded to the client) until all followers eventually
        # store all log entries.

        # A log entry is committed once the leader that created the 
        # entry has replicated it on a majority of the servers
        while successful_replications < self._server_count // 2:
            time.sleep(0.1)
        else:
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
        for command in commands:
            entries.append(
                Entry(
                    command=command,
                    term=self._leader.current_term,
                    index=next_index,
                )
            )
            next_index += 1

        return entries

    def _apply_entries(self, index: int) -> None:
        for i, entry in enumerate(self._log):
            if i <= index:
                self._state_machine[entry.command.key] = entry.command.value

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
            f"Candidate: {id(self)} recieved heartbeat for "
            f"term {term} and current term {self._current_term}"
        )
        if term < self._current_term:
            return self._current_term, False

        self._is_led = True
        self._leader_id = leader_id
        if self.state == RaftServerState.candidate:
            self.state == RaftServerState.follower

        if prev_log_index in range(len(self._log)):
            if self._log[prev_log_index].term != prev_log_term:
                return self._current_term, False
        else:
            return self._current_term, False

        copied_entries = list()
        for entry in entries:
            copied_entries.append(Entry.from_entry(entry))

        if prev_log_index + 1 in range(len(self._log)):
            for i in range(prev_log_index + 1, len(self._log)):
                if self._log[i].term != copied_entries[i - prev_log_index].term:
                    self._log = self._log[:i]
                    copied_entries = copied_entries[i:]
                    break

        self._log.extend(copied_entries)
        if leader_commit > self._commit_index:
            self._commit_index = min(leader_commit, len(self._log) - 1)

        if leader_commit > self._last_applied:
            self._last_applied += 1
            self._apply_entries()

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
        if term < self._current_term:
            return self._current_term, False

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
        return f"{self.__class__}, RaftServerState: {self.state}, id: {id(self)}"


class RaftCluster:
    def __init__(self, server_count=3):
        self._servers = [RaftServer(server_count) for _ in range(server_count)]
        for server in self._servers:
            other_servers = list(self._servers)
            other_servers.remove(server)
            server.other_servers = other_servers

    def start(self):
        for server in self._servers:
            server.start()

    def set(self, **kwargs):
        commands = [Command(key=k, value=v) for k, v in kwargs.items()]
        self._leader.set_entries(commands)


if __name__ == "__main__":
    raft_cluster = RaftCluster()
    raft_cluster.start()
    raft_cluster.set(x=3, y=5)

    while True:
        time.sleep(0.1)
    # logging.debug("test")