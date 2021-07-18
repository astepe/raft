import concurrent.futures as cf
from http import server
import logging
import random
import threading
import time
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import wraps
from threading import Thread
from typing import List, Tuple, Dict

import names

Command = namedtuple("Command", "key value")

logging.basicConfig(level=logging.INFO)

HEARTBEAT_INTERVAL = 0.03
ELECTION_TIMEOUT_INTERVAL_RANGE = (0.05, 0.075)
NETWORK_DELAY_INTERVAL_RANGE = (0.005, 0.0075)


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
        time.sleep(random.uniform(*NETWORK_DELAY_INTERVAL_RANGE))
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


class ElectionTimeout:
    """
    Acts as the election timeout mechanism. To prevent split votes,
    election timeouts are chosen randomly from a fixed interval of 150–300ms.
    """

    def __init__(self):
        self._timeout = threading.Event()
        self._timer = self._create_timer()

    def _create_timer(self):
        return threading.Timer(
            random.uniform(*ELECTION_TIMEOUT_INTERVAL_RANGE), self._timeout.set
        )

    def reset(self):
        self._timer.cancel()
        self._timer = self._create_timer()
        self._timer.start()

    def wait(self):
        self._timeout.wait()
        self._timeout.clear()



class Vote:
    """
    Vote values can only move from None
    to a candidate id value. This prevents votes
    from being changed after then are placed.
    """

    def __init__(self):
        self._value = None
        self._term = None
        self._voter = None

    @property
    def value(self):
        return self._value

    def set(self, value: str, term, voter):
        if self._value is None:
            self._value = value
            self._term = term
            self._voter = voter
        if value != self._value:
            raise ValueError(
                f"Voter {voter} tried to change {self._value} to {value} for term {term}"
            )
        
    def __repr__(self):
        return f"Vote: {self._value}, Voter: {self._voter}, Term: {self._term}"


class RaftServer:
    def __init__(self, cluster_events: List[threading.Event]):
        self._state: RaftServerState = RaftServerState.follower
        self._leader_id = None
        self._current_term = 0
        self._commit_index = 0
        self._next_indexes = dict()
        self._last_applied = 0
        self._servers = dict()
        self._log: List[Entry] = list()
        self._state_loop_thread = Thread(target=self._run_state_loop)
        self._events = {
            "stopped": threading.Event(),
        }
        self._cluster_events = cluster_events
        self._election_timeout = ElectionTimeout()
        self._request_thread_pool = ThreadPoolExecutor()
        self._state_machine = dict()
        self._id = names.get_first_name()
        self._term_votes = defaultdict(Vote)

    @property
    def id(self):
        """
        The unique id of the server
        """
        return self._id

    def start(self):
        self._running = True
        self._state_loop_thread.start()

    def stop(self):
        logging.info(f"Server: {self.id} stopping...")
        self._running = False
        self._events["stopped"].wait()

    @property
    def leader_id(self):
        if self._state == RaftServerState.leader:
            return self.id
        return self._leader_id

    @leader_id.setter
    def leader_id(self, leader_id):
        if self.state != RaftServerState.leader:
            self._leader_id = leader_id

    @property
    def other_servers(self) -> Dict:
        """
        Returns a dict of RaftServer instances within
        the cluster other than the current server

        Returns:
            dict: Maps server id to server instance
        """
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

    def _set_follower(self):
        self._state = RaftServerState.follower
        self._election_timeout.reset()

    def _run_state_loop(self):
        while self._running:
            if self._state == RaftServerState.follower:
                self._election_timeout.reset()
                self._election_timeout.wait()

            self._run_candidate()

            if self._state == RaftServerState.leader:
                self._run_leader()
        self._events["stopped"].set()

    def _run_candidate(self):
        """
        Runs the candidate routine. Attempts to gather
        a majority of votes from other nodes in order
        to become the new leader.
        """
        self._current_term += 1
        self._state = RaftServerState.candidate

        if self._term_votes[self._current_term].value is not None:
            self._set_follower()
            return

        self._term_votes[self._current_term].set(self.id, self._current_term, self.id)
        logging.debug(
            f"Candidate: {self.id} in term {self._current_term} voting for {self.id}."
        )
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
        results = [future.result() for future in cf.as_completed(futures)]
        vote_total = sum([result[1] for result in results]) + 1
        max_term = max([result[0] for result in results])

        if max_term > self._current_term:
            self._current_term = max_term
            logging.info(f"Candidate: {self.id} current term lower than term {max_term} from other server. Becoming follower.")
            self._set_follower()
        elif vote_total > len(self.servers) / 2:
            logging.info(
                f"Candidate: {self.id} elected as leader with {vote_total} votes in term {self._current_term}."
            )
            self._state = RaftServerState.leader
        else:
            logging.info(
                f"Candidate: {self.id} lost election with {vote_total} vote{'s' if vote_total != 1 else ''}."
            )
            self._set_follower()

    def _run_leader(self):
        """
        Runs the leader routine. Sends heartbeats to all other nodes
        to maintain leader status.
        """
        self._next_indexes = {
            server.id: len(self._log) for server in self.other_servers.values()
        }
        self._match_indexes = {server.id: 0 for server in self.other_servers.values()}
        while self._running and self._state == RaftServerState.leader:
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
            max_term = max([result[0] for result in results])
            if max_term > self._current_term:
                self._current_term = max_term
                logging.info(f"Leader: {self.id} current term lower than term {max_term} from other server. Becoming follower.")
                self._set_follower()
            else:
                self._cluster_events["leader_established"].set()
                time.sleep(HEARTBEAT_INTERVAL)
                logging.debug(f"Leader: {self.id} heartbeat {results}.")

        self._cluster_events["leader_established"].clear()
        logging.info(f"Server: {self.id} no longer leader.")

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
            self.leader.set_entries(commands)
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
                    self._set_follower()
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

        while successful_replications < len(self.servers) // 2:
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
        for command in commands:
            entries.append(
                Entry(
                    command=command,
                    term=self.leader.current_term,
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

    # @network_delay
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
            f"term {term} and current term {self._current_term} from {leader_id}"
        )
        if term < self._current_term:
            return self._current_term, False

        if term > self._current_term:
            self._current_term = term
        self._election_timeout.reset()
        self._state == RaftServerState.follower
        self.leader_id = leader_id
        logging.debug(f"Server: {self.id} is now following {leader_id}.")

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

    # @network_delay
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
            Tuple: current_term, for candidate to update itself and `vote_granted`,
             true means candidate received vote
        """
        # Reply false if term < currentTerm
        if term < self._current_term:
            return self._current_term, False
        
        if term > self._current_term:
            self._current_term = term
            self._set_follower()

        # If votedFor is null or candidateId, and candidate’s log is at
        # least as up-to-date as receiver’s log, grant vote
        if (
            self._term_votes[term].value is None
            or self._term_votes[term].value == candidate_id
        ) and len(self._log) <= last_log_index:
            self._term_votes[term].set(candidate_id, term, self.id)
            logging.info(f"Server: {self.id} in term {term} voting for {candidate_id}")
            return self._current_term, True

        return self._current_term, False

    @property
    def current_term(self):
        return self._current_term

    @property
    def leader(self):
        return self._servers[self.leader_id]

    @property
    def state(self):
        return self._state

    def __repr__(self):
        return f"{self.__class__}, RaftServerState: {self.state}, id: {self.id}"


class RaftCluster:
    def __init__(self, server_count=3):
        self._events = {
            "leader_established": threading.Event(),
            "stopped": threading.Event(),
        }
        self._servers = [
            RaftServer(cluster_events=self._events) for _ in range(server_count)
        ]
        for server in self._servers:
            server.servers = list(self._servers)
        self._thread_pool = ThreadPoolExecutor()

    def start(self):
        for server in self._servers:
            server.start()
        self._events["leader_established"].wait()
        self._events["stopped"].wait()

    def stop(self):
        futures = list()
        for server in self._servers:
            futures.append(self._thread_pool.submit(server.stop))
        cf.wait(futures)
        self._events["stopped"].set()

    @property
    def leader(self):
        return self._servers[0].leader

    @property
    def servers(self):
        return self._servers

    def set(self, **kwargs):
        commands = [Command(key=k, value=v) for k, v in kwargs.items()]
        self._servers[0].set_entries(commands)


if __name__ == "__main__":
    raft_cluster = RaftCluster()
    raft_cluster.start()
    # raft_cluster.set(x=3, y=5)
    raft_cluster.stop()
