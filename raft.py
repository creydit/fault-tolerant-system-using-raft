"""
raft.py - The Raft consensus algorithm (the heart of the project).

Raft makes all nodes agree on the same LOG: an ordered list of commands.
It has three jobs, and you will find one section of this file for each:

  PART 1  Leader election   - choose one leader; choose a new one if it dies
  PART 2  Log replication   - the leader copies commands to the followers
  PART 3  Persistence       - save important state to disk to survive a crash

Vocabulary:
  term      - a number that grows by 1 at every election ("election round").
              A message with an older term comes from the past and is ignored.
  majority  - more than half of the nodes (2 out of 3).
  committed - a log entry stored on a majority. It can never be lost.
  applied   - a committed entry that has been executed on the key-value store.
"""

import json
import os
import random
import threading
import time

from config import (
    NODES,
    HEARTBEAT_INTERVAL,
    ELECTION_TIMEOUT_MIN,
    ELECTION_TIMEOUT_MAX,
    RPC_TIMEOUT,
    CLIENT_REQUEST_TIMEOUT,
)
from rpc import send_rpc

# The three roles a node can be in.
FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class RaftNode:
    def __init__(self, node_id, apply_command, data_folder):
        self.node_id = node_id
        self.peer_ids = [other for other in NODES if other != node_id]

        # Function we call to execute a committed command on the database.
        self.apply_command = apply_command

        # Several threads use this object at the same time (the timer thread
        # and one thread per incoming message). The lock makes sure only one
        # of them touches our variables at a time.
        self.lock = threading.Lock()

        # ---- Persistent state: saved to disk, survives a crash ----
        self.current_term = 0
        self.voted_for = None  # who we voted for in current_term
        # The log. Entry 0 is a dummy so that real entries start at index 1,
        # exactly like in the Raft paper.
        self.log = [{"term": 0, "command": None}]

        # ---- Volatile state: lost on a crash, and that is fine ----
        self.role = FOLLOWER
        self.leader_id = None      # who we believe the leader is
        self.commit_index = 0      # highest log index known to be committed
        self.last_applied = 0      # highest log index executed on the database
        self.votes_received = 0    # used only while we are a candidate
        self.next_index = {}       # leader only: next entry to send to each peer
        self.match_index = {}      # leader only: highest entry each peer has
        self.results = {}          # log index -> result, for waiting clients
        self.last_heartbeat_sent = 0.0
        self.election_deadline = 0.0

        # Testing only: peers we pretend we cannot reach (a fake network cut).
        self.blocked_peers = set()

        os.makedirs(data_folder, exist_ok=True)
        self.state_file = os.path.join(data_folder, "node_" + str(node_id) + ".json")
        self.load_from_disk()
        self.reset_election_timer()

    def start(self):
        """Start the background timer thread."""
        thread = threading.Thread(target=self.run_timer_loop, daemon=True)
        thread.start()

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------

    def print_message(self, text):
        clock = time.strftime("%H:%M:%S")
        print("[" + clock + "] [node " + str(self.node_id) + "] " + text, flush=True)

    def last_log_index(self):
        return len(self.log) - 1

    def last_log_term(self):
        return self.log[-1]["term"]

    def reset_election_timer(self):
        """Pick a new random moment at which we will start an election."""
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self.election_deadline = time.monotonic() + timeout

    def send_to_peer(self, peer_id, method, args):
        if peer_id in self.blocked_peers:
            return None  # pretend the network is cut
        return send_rpc(NODES[peer_id], method, args, RPC_TIMEOUT)

    def run_timer_loop(self):
        """
        Runs forever in the background and checks two clocks:
          - if I am the leader:   is it time to send heartbeats?
          - if I am not a leader: have I waited too long for a heartbeat?
        """
        while True:
            time.sleep(0.02)
            with self.lock:
                now = time.monotonic()
                role = self.role
                heartbeat_due = now - self.last_heartbeat_sent >= HEARTBEAT_INTERVAL
                election_due = now >= self.election_deadline

            if role == LEADER and heartbeat_due:
                self.send_append_entries_to_all()
            if role != LEADER and election_due:
                self.start_election()

    def become_follower(self, term):
        """Step down. Called whenever we see a term at least as new as ours."""
        if self.role != FOLLOWER:
            self.print_message("stepping down to FOLLOWER (term " + str(term) + ")")
        self.role = FOLLOWER
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None  # new term: we have not voted in it yet
            self.save_to_disk()

    # ==================================================================
    # PART 1: LEADER ELECTION
    # ==================================================================

    def start_election(self):
        """We heard nothing from a leader for too long: ask to be the leader."""
        with self.lock:
            self.role = CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id  # vote for ourselves
            self.votes_received = 1
            self.leader_id = None
            self.save_to_disk()
            self.reset_election_timer()
            self.print_message("no leader heard: starting ELECTION for term "
                               + str(self.current_term))
            args = {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self.last_log_index(),
                "last_log_term": self.last_log_term(),
            }

        # Ask every other node for its vote, all at the same time.
        for peer_id in self.peer_ids:
            thread = threading.Thread(target=self.ask_for_vote, args=(peer_id, args),
                                      daemon=True)
            thread.start()

    def ask_for_vote(self, peer_id, args):
        reply = self.send_to_peer(peer_id, "request_vote", args)
        if reply is None:
            return  # peer is down or unreachable

        with self.lock:
            # The peer is in a newer term: our election is out of date.
            if reply["term"] > self.current_term:
                self.become_follower(reply["term"])
                return

            # Ignore the reply if this election is already over.
            still_running = (self.role == CANDIDATE and self.current_term == args["term"])
            if not still_running:
                return

            if reply["vote_granted"]:
                self.votes_received += 1
                if self.votes_received > len(NODES) / 2:
                    self.become_leader()

    def become_leader(self):
        self.role = LEADER
        self.leader_id = self.node_id
        self.print_message("won the election: I am the LEADER for term "
                           + str(self.current_term))

        # We assume every follower's log is as long as ours. If that is
        # wrong, replicate_to_peer() will find out and step backwards.
        for peer_id in self.peer_ids:
            self.next_index[peer_id] = self.last_log_index() + 1
            self.match_index[peer_id] = 0

        # Add an empty "no-op" entry. Raft only lets a leader commit entries
        # from its OWN term, so this lets older entries get committed quickly.
        self.log.append({"term": self.current_term, "command": {"op": "noop"}})
        self.save_to_disk()

        # Makes the timer loop send heartbeats immediately.
        self.last_heartbeat_sent = 0.0

    def handle_request_vote(self, args):
        """Another node is asking for our vote. Decide yes or no."""
        with self.lock:
            if args["candidate_id"] in self.blocked_peers:
                return None

            if args["term"] > self.current_term:
                self.become_follower(args["term"])

            vote_granted = False
            if args["term"] == self.current_term:
                # Rule 1: only ONE vote per term.
                not_voted_yet = (self.voted_for is None
                                 or self.voted_for == args["candidate_id"])

                # Rule 2: only vote for a candidate whose log is at least as
                # up to date as ours. This guarantees that the winner already
                # has every committed entry, so nothing committed is lost.
                if args["last_log_term"] != self.last_log_term():
                    candidate_log_ok = args["last_log_term"] > self.last_log_term()
                else:
                    candidate_log_ok = args["last_log_index"] >= self.last_log_index()

                if not_voted_yet and candidate_log_ok:
                    vote_granted = True
                    self.voted_for = args["candidate_id"]
                    self.save_to_disk()
                    self.reset_election_timer()
                    self.print_message("voted for node " + str(args["candidate_id"])
                                       + " in term " + str(self.current_term))

            return {"term": self.current_term, "vote_granted": vote_granted}

    # ==================================================================
    # PART 2: LOG REPLICATION
    # ==================================================================

    def submit_command(self, command):
        """
        A client wants to run a command (put/get/...).
        Only the leader accepts it. The leader:
          1. adds it to its own log
          2. copies it to the followers
          3. waits until a majority has it (committed) and it was executed
          4. only THEN answers the client
        """
        with self.lock:
            if self.role != LEADER:
                return {"ok": False, "error": "not_leader", "leader_id": self.leader_id}

            self.log.append({"term": self.current_term, "command": command})
            self.save_to_disk()
            index = self.last_log_index()
            term = self.current_term
            self.results[index] = None  # "a client is waiting for this index"

        self.send_append_entries_to_all()

        # Wait for the entry to be committed and applied.
        deadline = time.monotonic() + CLIENT_REQUEST_TIMEOUT
        while time.monotonic() < deadline:
            with self.lock:
                if self.last_applied >= index:
                    result = self.results.pop(index)
                    # Make sure the entry at this index is still OURS. If we
                    # lost leadership, a new leader may have replaced it.
                    if self.log[index]["term"] == term:
                        return {"ok": True, "result": result}
                    return {"ok": False, "error": "not_leader",
                            "leader_id": self.leader_id}
            time.sleep(0.005)

        # We could not reach a majority in time.
        with self.lock:
            self.results.pop(index, None)
            return {"ok": False, "error": "timeout", "leader_id": self.leader_id}

    def send_append_entries_to_all(self):
        """Leader: send new log entries (or an empty heartbeat) to everyone."""
        with self.lock:
            self.last_heartbeat_sent = time.monotonic()
        for peer_id in self.peer_ids:
            thread = threading.Thread(target=self.replicate_to_peer, args=(peer_id,),
                                      daemon=True)
            thread.start()

    def replicate_to_peer(self, peer_id):
        """Leader: bring ONE follower's log up to date with ours."""
        with self.lock:
            if self.role != LEADER:
                return
            term_when_sent = self.current_term
            next_index = self.next_index[peer_id]
            previous_index = next_index - 1
            entries = self.log[next_index:]  # empty list = pure heartbeat
            args = {
                "term": self.current_term,
                "leader_id": self.node_id,
                # The entry just BEFORE the new ones. The follower uses it to
                # check that its log matches ours up to that point.
                "prev_log_index": previous_index,
                "prev_log_term": self.log[previous_index]["term"],
                "entries": entries,
                "leader_commit": self.commit_index,
            }

        reply = self.send_to_peer(peer_id, "append_entries", args)
        if reply is None:
            return  # follower is down; we simply try again at the next heartbeat

        with self.lock:
            if reply["term"] > self.current_term:
                self.become_follower(reply["term"])
                return
            if self.role != LEADER or self.current_term != term_when_sent:
                return  # things changed while we were waiting for the reply

            if reply["success"]:
                # The follower now has everything we sent.
                matched = previous_index + len(entries)
                self.match_index[peer_id] = max(self.match_index[peer_id], matched)
                self.next_index[peer_id] = self.match_index[peer_id] + 1
                self.advance_commit_index()
            else:
                # The follower's log does not match ours at previous_index.
                # Step backwards and try again with older entries. If the
                # follower's log is simply shorter, jump straight to its end.
                new_next = min(next_index - 1, reply["last_log_index"] + 1)
                self.next_index[peer_id] = max(1, new_next)

    def advance_commit_index(self):
        """Leader: an entry stored on a MAJORITY of nodes becomes committed."""
        index = self.last_log_index()
        while index > self.commit_index:
            # A leader may only commit entries from its own term (Raft paper,
            # section 5.4.2). Older entries get committed along with them.
            if self.log[index]["term"] != self.current_term:
                break

            nodes_with_entry = 1  # the leader itself
            for peer_id in self.peer_ids:
                if self.match_index[peer_id] >= index:
                    nodes_with_entry += 1

            if nodes_with_entry > len(NODES) / 2:
                self.commit_index = index
                self.apply_committed_entries()
                break
            index -= 1

    def handle_append_entries(self, args):
        """Follower: the leader sent us log entries (or just a heartbeat)."""
        with self.lock:
            if args["leader_id"] in self.blocked_peers:
                return None

            # A message from an old leader that does not know it was replaced.
            if args["term"] < self.current_term:
                return {"term": self.current_term, "success": False,
                        "last_log_index": self.last_log_index()}

            # This is the real leader of the current term: follow it.
            self.become_follower(args["term"])
            self.leader_id = args["leader_id"]
            self.reset_election_timer()

            # Consistency check: do we have the entry that comes just before
            # the new ones, and with the same term? If not, our log differs
            # from the leader's and we must refuse.
            previous_index = args["prev_log_index"]
            if (previous_index > self.last_log_index()
                    or self.log[previous_index]["term"] != args["prev_log_term"]):
                return {"term": self.current_term, "success": False,
                        "last_log_index": self.last_log_index()}

            # Add the new entries to our log.
            log_changed = False
            index = previous_index
            for entry in args["entries"]:
                index += 1
                if index <= self.last_log_index():
                    if self.log[index]["term"] == entry["term"]:
                        continue  # we already have this entry
                    # We have a DIFFERENT entry here (from an old leader that
                    # failed). The leader is always right: delete ours and
                    # everything after it.
                    del self.log[index:]
                self.log.append(entry)
                log_changed = True

            if log_changed:
                self.save_to_disk()

            # The leader tells us how far it has committed; we follow.
            last_new_index = previous_index + len(args["entries"])
            new_commit_index = min(args["leader_commit"], last_new_index)
            if new_commit_index > self.commit_index:
                self.commit_index = new_commit_index
                self.apply_committed_entries()

            return {"term": self.current_term, "success": True,
                    "last_log_index": self.last_log_index()}

    def apply_committed_entries(self):
        """Execute every committed entry that we have not executed yet."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            command = self.log[self.last_applied]["command"]

            result = None
            if command["op"] != "noop":
                result = self.apply_command(command)

            # If a client is waiting for this entry, leave the result for it.
            if self.last_applied in self.results:
                self.results[self.last_applied] = result

    # ==================================================================
    # PART 3: PERSISTENCE
    # ==================================================================

    def save_to_disk(self):
        """
        Save term, vote and log. We must do this BEFORE answering any
        message, otherwise a crash could make us forget a promise
        (for example, vote twice in the same term).
        """
        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log,
        }
        # Write to a temporary file first and then rename it. A crash in the
        # middle of writing can then never leave a half-written state file.
        temporary_file = self.state_file + ".tmp"
        with open(temporary_file, "w") as file:
            json.dump(state, file)
            file.flush()
            os.fsync(file.fileno())  # force the data onto the disk
        os.replace(temporary_file, self.state_file)

    def load_from_disk(self):
        """After a restart, continue from where we stopped."""
        if not os.path.exists(self.state_file):
            return  # first start ever: nothing to load
        with open(self.state_file) as file:
            state = json.load(file)
        self.current_term = state["current_term"]
        self.voted_for = state["voted_for"]
        self.log = state["log"]
        self.print_message("loaded state from disk: term " + str(self.current_term)
                           + ", " + str(self.last_log_index()) + " log entries")
        # Note: the key-value data itself is NOT saved. It is rebuilt by
        # replaying the log once the leader tells us what is committed.

    # ------------------------------------------------------------------
    # For debugging and testing
    # ------------------------------------------------------------------

    def get_status(self):
        with self.lock:
            return {
                "node_id": self.node_id,
                "role": self.role,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "log_length": self.last_log_index(),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
            }

    def set_blocked_peers(self, peer_ids):
        with self.lock:
            self.blocked_peers = set(peer_ids)
            self.print_message("network cut: now blocking nodes " + str(sorted(peer_ids)))
