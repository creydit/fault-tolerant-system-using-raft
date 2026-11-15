"""
config.py - Settings shared by every file in the project.

Our cluster has 3 servers (called "nodes"). All of them run on your own
computer, but each one listens on a different port, so they behave like
3 separate machines talking over a network.
"""

# node id -> (host, port)
NODES = {
    1: ("127.0.0.1", 5001),
    2: ("127.0.0.1", 5002),
    3: ("127.0.0.1", 5003),
}

# The leader sends a heartbeat to every follower this often (seconds).
HEARTBEAT_INTERVAL = 0.1

# If a follower hears nothing from a leader for this long, it starts an
# election. Each node picks a RANDOM time in this range, so that two nodes
# rarely start an election at the same moment.
ELECTION_TIMEOUT_MIN = 0.5
ELECTION_TIMEOUT_MAX = 1.0

# How long one node waits for another node to answer a message (seconds).
RPC_TIMEOUT = 0.3

# How long the leader waits for a client's command to be copied to a
# majority before giving up (seconds).
CLIENT_REQUEST_TIMEOUT = 3.0
