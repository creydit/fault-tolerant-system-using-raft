"""
kv_store.py - The actual "database": a dictionary of key -> value.

This file knows NOTHING about networks or Raft. Raft calls apply() once
for every committed command, in the same order on every node. Because
every node starts empty and applies the same commands in the same order,
every node ends up with the same data. (This idea is called a
"replicated state machine".)
"""


class KeyValueStore:
    def __init__(self):
        self.data = {}

        # Remembers the last request of each client, so that a command
        # that was sent twice (because of a retry) is only executed once.
        # client_id -> {"request_id": ..., "result": ...}
        self.last_request = {}

    def apply(self, command):
        """Execute one command and return its result."""
        client_id = command["client_id"]
        request_id = command["request_id"]

        # Duplicate detection: have we already executed this exact request?
        previous = self.last_request.get(client_id)
        if previous is not None and previous["request_id"] == request_id:
            return previous["result"]

        op = command["op"]
        key = command["key"]
        value = command.get("value")

        if op == "put":
            self.data[key] = value
            result = "OK"
        elif op == "append":
            self.data[key] = self.data.get(key, "") + value
            result = "OK"
        elif op == "delete":
            self.data.pop(key, None)
            result = "OK"
        elif op == "get":
            result = self.data.get(key)  # None if the key does not exist
        else:
            result = "ERROR: unknown operation " + str(op)

        self.last_request[client_id] = {"request_id": request_id, "result": result}
        return result
