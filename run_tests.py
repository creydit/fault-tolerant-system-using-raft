"""
run_tests.py - Automatic tests for the whole system.

Usage: python run_tests.py

Each test starts a fresh cluster (one real process per node in config.py), does
something nasty to it (kills nodes, cuts the network, ...) and checks
that the system still behaves correctly.

IMPORTANT: stop any servers you started by hand before running this,
because the tests use the same ports.
"""

import os
import shutil
import subprocess
import sys
import threading
import time

from client import KVClient
from config import NODES
from rpc import send_rpc

PROJECT_FOLDER = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_FOLDER = os.path.join(PROJECT_FOLDER, "test_data")

ALL_NODES = sorted(NODES)  # for example [1, 2, 3]

processes = {}  # node_id -> running server process


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------

def start_node(node_id):
    os.makedirs(TEST_DATA_FOLDER, exist_ok=True)
    log_file = open(os.path.join(TEST_DATA_FOLDER, "node_" + str(node_id) + ".log"), "a")
    processes[node_id] = subprocess.Popen(
        [sys.executable, "server.py", str(node_id), TEST_DATA_FOLDER],
        cwd=PROJECT_FOLDER, stdout=log_file, stderr=subprocess.STDOUT)


def stop_node(node_id):
    """Kill the process immediately, like a real crash."""
    if node_id in processes:
        processes[node_id].kill()
        processes[node_id].wait()
        del processes[node_id]


def stop_all_nodes():
    for node_id in list(processes):
        stop_node(node_id)


def start_fresh_cluster():
    stop_all_nodes()
    shutil.rmtree(TEST_DATA_FOLDER, ignore_errors=True)
    for node_id in NODES:
        start_node(node_id)


def get_status(node_id):
    return send_rpc(NODES[node_id], "status", {}, 1.0)


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def wait_until(condition, timeout, message):
    """Call condition() again and again until it returns True."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.1)
    raise AssertionError("timed out waiting: " + message)


def find_leaders(node_ids):
    leaders = []
    for node_id in node_ids:
        status = get_status(node_id)
        if status is not None and status["role"] == "leader":
            leaders.append(node_id)
    return leaders


def wait_for_leader(node_ids):
    """Wait until exactly one of the given nodes is leader; return its id."""
    wait_until(lambda: len(find_leaders(node_ids)) == 1, 10,
               "exactly one leader among nodes " + str(node_ids))
    return find_leaders(node_ids)[0]


def all_nodes_have_data(node_ids, expected_data):
    for node_id in node_ids:
        status = get_status(node_id)
        if status is None or status["data"] != expected_data:
            return False
    return True


# ----------------------------------------------------------------------
# The tests
# ----------------------------------------------------------------------

def test_leader_election():
    """A fresh cluster elects exactly one leader."""
    start_fresh_cluster()
    leader = wait_for_leader(ALL_NODES)
    print("    leader is node " + str(leader))
    time.sleep(2)
    check(find_leaders(ALL_NODES) == [leader], "the leader should stay stable")


def test_put_and_get():
    """Basic operations work."""
    start_fresh_cluster()
    client = KVClient()
    check(client.put("name", "alice") == "OK", "put failed")
    check(client.get("name") == "alice", "get returned the wrong value")
    client.put("name", "bob")
    check(client.get("name") == "bob", "overwrite failed")
    client.append("name", "!")
    check(client.get("name") == "bob!", "append failed")
    client.delete("name")
    check(client.get("name") is None, "delete failed")


def test_replication():
    """Data written through the leader appears on ALL nodes."""
    start_fresh_cluster()
    client = KVClient()
    client.put("a", "1")
    client.put("b", "2")
    wait_until(lambda: all_nodes_have_data(ALL_NODES, {"a": "1", "b": "2"}), 5,
               "all nodes to hold the same data")


def test_leader_crash():
    """FAULT TOLERANCE: kill the leader; a new one takes over, no data lost."""
    start_fresh_cluster()
    client = KVClient()
    client.put("city", "delhi")

    old_leader = wait_for_leader(ALL_NODES)
    print("    killing leader node " + str(old_leader))
    stop_node(old_leader)

    survivors = [n for n in NODES if n != old_leader]
    new_leader = wait_for_leader(survivors)
    print("    new leader is node " + str(new_leader))
    check(new_leader != old_leader, "a different node must become leader")

    check(client.get("city") == "delhi", "data was lost when the leader crashed")
    check(client.put("city", "mumbai") == "OK", "cannot write after leader crash")
    check(client.get("city") == "mumbai", "wrong value after leader crash")


def test_follower_catches_up():
    """REPLICATION: a node that was down gets the entries it missed."""
    start_fresh_cluster()
    client = KVClient()
    client.put("k0", "v0")
    leader = wait_for_leader(ALL_NODES)
    follower = [n for n in NODES if n != leader][0]

    print("    killing follower node " + str(follower))
    stop_node(follower)
    expected = {"k0": "v0"}
    for i in range(1, 21):
        client.put("k" + str(i), "v" + str(i))
        expected["k" + str(i)] = "v" + str(i)

    print("    restarting node " + str(follower) + " (it missed 20 writes)")
    start_node(follower)
    wait_until(lambda: all_nodes_have_data(ALL_NODES, expected), 10,
               "the restarted follower to catch up")


def test_persistence():
    """PERSISTENCE: kill ALL nodes, restart them, the data is still there."""
    start_fresh_cluster()
    client = KVClient()
    client.put("x", "100")
    client.put("y", "200")

    print("    killing all nodes")
    stop_all_nodes()
    print("    restarting all nodes")
    for node_id in NODES:
        start_node(node_id)

    check(client.get("x") == "100", "x was lost after a full restart")
    check(client.get("y") == "200", "y was lost after a full restart")


def test_no_majority():
    """CONSISTENCY: with only 1 node alive, no write may be confirmed."""
    start_fresh_cluster()
    client = KVClient()
    client.put("safe", "yes")
    leader = wait_for_leader(ALL_NODES)
    others = [n for n in NODES if n != leader]

    print("    killing all followers; only node " + str(leader) + " is left")
    for node_id in others:
        stop_node(node_id)

    command = {"op": "put", "key": "unsafe", "value": "oops",
               "client_id": "test-no-majority", "request_id": 1}
    reply = send_rpc(NODES[leader], "client_request", command, 6)
    check(reply is None or not reply["ok"],
          "a write succeeded without a majority - this is unsafe!")
    status = get_status(leader)
    check("unsafe" not in status["data"],
          "a write was applied without a majority - this is unsafe!")
    print("    write was not confirmed and not applied")

    print("    restarting the followers")
    for node_id in others:
        start_node(node_id)
    check(client.put("after", "ok") == "OK", "cluster did not recover")
    check(client.get("safe") == "yes", "old data lost")
    # Note: the unconfirmed write may or may not complete now that a
    # majority is back. Both are correct: the client was never told "OK".
    # What matters is that all nodes agree on the same answer.
    final_data = get_status(wait_for_leader(ALL_NODES))["data"]
    wait_until(lambda: all_nodes_have_data(ALL_NODES, final_data), 5,
               "all nodes to agree after recovery")


def test_network_partition():
    """CONSISTENCY: a leader cut off from the others cannot change anything."""
    start_fresh_cluster()
    client = KVClient()
    client.put("color", "red")
    old_leader = wait_for_leader(ALL_NODES)
    others = [n for n in NODES if n != old_leader]

    print("    cutting leader node " + str(old_leader) + " off from the network")
    send_rpc(NODES[old_leader], "block", {"peers": others}, 1.0)

    # A write sent to the isolated leader must fail.
    command = {"op": "put", "key": "color", "value": "WRONG",
               "client_id": "test-partition", "request_id": 1}
    reply = send_rpc(NODES[old_leader], "client_request", command, 6)
    check(reply is None or not reply["ok"], "isolated leader accepted a write!")
    print("    isolated leader correctly refused a write")

    # The majority side elects a new leader and keeps working.
    new_leader = wait_for_leader(others)
    print("    majority side elected node " + str(new_leader))
    client.leader_id = new_leader
    check(client.put("color", "blue") == "OK", "majority side cannot write")

    # Heal the network: the old leader must step down and copy the new data.
    print("    healing the network")
    send_rpc(NODES[old_leader], "block", {"peers": []}, 1.0)
    wait_until(lambda: all_nodes_have_data(ALL_NODES, {"color": "blue"}), 10,
               "all nodes to agree after the partition healed")
    check(len(find_leaders(ALL_NODES)) == 1, "there must be one leader after healing")


def test_duplicate_request():
    """CONSISTENCY: the same request sent twice is executed only once."""
    start_fresh_cluster()
    leader = wait_for_leader(ALL_NODES)
    command = {"op": "append", "key": "word", "value": "ha",
               "client_id": "test-duplicate", "request_id": 1}
    first = send_rpc(NODES[leader], "client_request", command, 5)
    second = send_rpc(NODES[leader], "client_request", command, 5)  # a "retry"
    check(first["ok"] and second["ok"], "requests failed")
    value = KVClient().get("word")
    check(value == "ha", "expected 'ha' but got '" + str(value) + "' (executed twice?)")


def test_concurrent_clients():
    """CONSISTENCY: many clients at once; every write is applied exactly once."""
    start_fresh_cluster()
    wait_for_leader(ALL_NODES)
    number_of_clients = 5
    writes_per_client = 10
    errors = []

    def run_client(client_number):
        try:
            client = KVClient()
            for i in range(writes_per_client):
                client.append("shared", "[" + str(client_number) + ":" + str(i) + "]")
        except Exception as error:
            errors.append(error)

    threads = []
    for client_number in range(number_of_clients):
        thread = threading.Thread(target=run_client, args=(client_number,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    check(errors == [], "a client failed: " + str(errors))

    value = KVClient().get("shared")
    for client_number in range(number_of_clients):
        previous_position = -1
        for i in range(writes_per_client):
            piece = "[" + str(client_number) + ":" + str(i) + "]"
            check(value.count(piece) == 1, piece + " should appear exactly once")
            # Each client's own writes must appear in the order it sent them.
            position = value.index(piece)
            check(position > previous_position, piece + " is out of order")
            previous_position = position
    print("    50 concurrent writes, each applied exactly once and in order")

    wait_until(lambda: all_nodes_have_data(ALL_NODES, {"shared": value}), 5,
               "all nodes to hold identical data")


ALL_TESTS = [
    test_leader_election,
    test_put_and_get,
    test_replication,
    test_leader_crash,
    test_follower_catches_up,
    test_persistence,
    test_no_majority,
    test_network_partition,
    test_duplicate_request,
    test_concurrent_clients,
]


def main():
    passed = 0
    start_time = time.monotonic()
    for number, test in enumerate(ALL_TESTS, start=1):
        print("TEST " + str(number) + ": " + test.__name__)
        print("    " + test.__doc__.strip())
        try:
            test()
            print("    PASS\n")
            passed += 1
        except Exception as error:
            print("    FAIL: " + str(error) + "\n")
        finally:
            stop_all_nodes()

    seconds = round(time.monotonic() - start_time, 1)
    print("=" * 50)
    print("RESULT: " + str(passed) + " of " + str(len(ALL_TESTS)) + " tests passed ("
          + str(seconds) + " seconds)")
    print("=" * 50)
    sys.exit(0 if passed == len(ALL_TESTS) else 1)


if __name__ == "__main__":
    main()
