"""
server.py - Runs ONE node of the cluster.

Usage:   python server.py <node_id>
Example: python server.py 1

Start it three times (ids 1, 2 and 3) in three terminals to get a cluster.
Press Ctrl+C to "crash" a node.
"""

import sys
import time

from config import NODES
from kv_store import KeyValueStore
from raft import RaftNode
from rpc import start_rpc_server


def main():
    if len(sys.argv) < 2 or not sys.argv[1].isdigit() or int(sys.argv[1]) not in NODES:
        print("Usage: python server.py <node_id>   (node_id is 1, 2 or 3)")
        sys.exit(1)

    node_id = int(sys.argv[1])
    # Optional second argument: folder for the saved state (tests use this).
    data_folder = sys.argv[2] if len(sys.argv) > 2 else "data"

    store = KeyValueStore()
    raft = RaftNode(node_id, store.apply, data_folder)

    def handle_request(method, args):
        """Every incoming message arrives here and is sent to the right place."""
        # Messages from other nodes
        if method == "request_vote":
            return raft.handle_request_vote(args)
        if method == "append_entries":
            return raft.handle_append_entries(args)

        # Messages from clients
        if method == "client_request":
            return raft.submit_command(args)

        # Messages for debugging / testing
        if method == "status":
            status = raft.get_status()
            with raft.lock:
                status["data"] = dict(store.data)
            return status
        if method == "block":
            raft.set_blocked_peers(args["peers"])
            return {"ok": True}

        return {"ok": False, "error": "unknown method"}

    start_rpc_server(NODES[node_id], handle_request)
    raft.start()
    raft.print_message("started on port " + str(NODES[node_id][1]) + " as FOLLOWER")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        raft.print_message("stopped")


if __name__ == "__main__":
    main()
