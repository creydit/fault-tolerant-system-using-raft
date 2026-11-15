"""
rpc.py - How nodes talk to each other.

RPC means "Remote Procedure Call": asking ANOTHER program to run a function
and send back the answer.

Our protocol is as simple as possible:
  1. Open a TCP connection to the other node.
  2. Send ONE line of JSON:   {"method": "request_vote", "args": {...}}
  3. Read ONE line of JSON back (the reply).
  4. Close the connection.
"""

import json
import os
import socket
import socketserver
import threading


def send_rpc(address, method, args, timeout):
    """
    Send a request to the node at `address` and return its reply.

    Returns None if anything goes wrong (node is down, too slow, ...).
    In a distributed system failure is normal, so callers must always
    be ready for None.
    """
    try:
        with socket.create_connection(address, timeout=timeout) as connection:
            request = {"method": method, "args": args}
            connection.sendall((json.dumps(request) + "\n").encode("utf-8"))

            reader = connection.makefile("r", encoding="utf-8")
            line = reader.readline()
            if line == "":
                return None  # the other side closed without answering
            return json.loads(line)
    except (OSError, ValueError):
        # OSError    = connection refused / timed out / broken
        # ValueError = the reply was not valid JSON
        return None


def start_rpc_server(address, handle_request):
    """
    Start listening on `address` in a background thread.

    Every time a request arrives we call:
        reply = handle_request(method, args)
    and send `reply` back to the caller.
    """

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            line = self.rfile.readline()
            if not line:
                return
            request = json.loads(line.decode("utf-8"))
            reply = handle_request(request["method"], request["args"])
            self.wfile.write((json.dumps(reply) + "\n").encode("utf-8"))

    class Server(socketserver.ThreadingTCPServer):
        # Each request is handled in its own thread, so a slow request
        # does not block the others.
        daemon_threads = True
        # Lets us restart a node immediately on the same port (Linux/Mac).
        allow_reuse_address = (os.name != "nt")

    server = Server(address, Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
