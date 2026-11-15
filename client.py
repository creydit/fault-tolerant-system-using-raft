"""
client.py - Talks to the cluster.

Usage:
    python client.py put <key> <value>
    python client.py get <key>
    python client.py append <key> <value>
    python client.py delete <key>
    python client.py status
"""

import sys
import time
import uuid

from config import NODES, CLIENT_REQUEST_TIMEOUT
from rpc import send_rpc


class KVClient:
    def __init__(self):
        # A unique name for this client and a counter for its requests.
        # Together they identify each request, so the servers can detect
        # a request that was sent twice.
        self.client_id = str(uuid.uuid4())
        self.request_id = 0
        self.leader_id = 1  # first guess; we will learn the real leader

    def send_command(self, op, key, value=None):
        self.request_id += 1
        command = {
            "op": op,
            "key": key,
            "value": value,
            "client_id": self.client_id,
            "request_id": self.request_id,
        }

        # Keep trying until the leader answers. We re-send the SAME
        # request_id every time, so a retry can never execute twice.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            reply = send_rpc(NODES[self.leader_id], "client_request", command,
                             CLIENT_REQUEST_TIMEOUT + 1)

            if reply is not None and reply["ok"]:
                return reply["result"]

            # It failed. Did the node tell us who the real leader is?
            hint = None
            if reply is not None and reply["error"] == "not_leader":
                hint = reply["leader_id"]

            if hint is not None and hint != self.leader_id:
                self.leader_id = hint
            else:
                # No useful hint: try the next node in the list.
                node_ids = sorted(NODES)
                position = node_ids.index(self.leader_id)
                self.leader_id = node_ids[(position + 1) % len(node_ids)]
                time.sleep(0.1)

        raise RuntimeError("The cluster did not answer. Is a majority of nodes running?")

    def put(self, key, value):
        return self.send_command("put", key, value)

    def get(self, key):
        return self.send_command("get", key)

    def append(self, key, value):
        return self.send_command("append", key, value)

    def delete(self, key):
        return self.send_command("delete", key)


def print_status():
    """Ask every node for its state and print one line per node."""
    for node_id in sorted(NODES):
        status = send_rpc(NODES[node_id], "status", {}, 1.0)
        if status is None:
            print("node " + str(node_id) + ": DOWN")
        else:
            print("node " + str(node_id) + ": " + status["role"].upper().ljust(9)
                  + " term=" + str(status["term"])
                  + " log_length=" + str(status["log_length"])
                  + " committed=" + str(status["commit_index"])
                  + " data=" + str(status["data"]))


def main():
    arguments = sys.argv[1:]
    client = KVClient()

    if len(arguments) == 1 and arguments[0] == "status":
        print_status()
    elif len(arguments) == 3 and arguments[0] == "put":
        print(client.put(arguments[1], arguments[2]))
    elif len(arguments) == 3 and arguments[0] == "append":
        print(client.append(arguments[1], arguments[2]))
    elif len(arguments) == 2 and arguments[0] == "get":
        print(client.get(arguments[1]))
    elif len(arguments) == 2 and arguments[0] == "delete":
        print(client.delete(arguments[1]))
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
