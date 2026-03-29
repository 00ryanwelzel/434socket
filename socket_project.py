#!/usr/bin/env python3
import csv
import gzip
import json
import math
import os
import random
import socket
import sys
import threading
import time
import uuid
from pathlib import Path

SUCCESS = "OK"
FAIL = "FAIL"

STATE_FREE = "FREE"
STATE_LEADER = "LEADER"
STATE_INDHT = "INDHT"

WAIT_NONE = "NONE"
WAIT_DHT_COMPLETE = "WAIT_DHT_COMPLETE"
WAIT_DHT_REBUILT = "WAIT_DHT_REBUILT"
WAIT_TEARDOWN_COMPLETE = "WAIT_TEARDOWN_COMPLETE"

FIELD_ORDER = [
    "event_id", "state", "year", "month", "event_type", "cz_type", "cz_name",
    "inj_direct", "inj_indirect", "death_direct", "death_indirect",
    "damage_property", "damage_crops", "tor_f_scale",
]

FIELD_LABELS = {
    "event_id": "event id",
    "state": "state",
    "year": "year",
    "month": "month name",
    "event_type": "event type",
    "cz_type": "cz type",
    "cz_name": "cz name",
    "inj_direct": "injuries direct",
    "inj_indirect": "injuries indirect",
    "death_direct": "deaths direct",
    "death_indirect": "deaths indirect",
    "damage_property": "damage property",
    "damage_crops": "damage crops",
    "tor_f_scale": "tor f scale",
}


def next_prime(x):
    if x <= 2:
        return 2

    def is_prime(n):
        if n % 2 == 0:
            return n == 2
        for i in range(3, int(math.isqrt(n)) + 1, 2):
            if n % i == 0:
                return False
        return True

    n = x if x % 2 else x + 1
    while not is_prime(n):
        n += 2
    return n


class PeerTuple:
    def __init__(self, name, ip, pport):
        self.name = name
        self.ip = ip
        self.pport = int(pport)


class PeerInfo:
    def __init__(self, name, ip, mport, pport, state=STATE_FREE):
        self.name = name
        self.ip = ip
        self.mport = int(mport)
        self.pport = int(pport)
        self.state = state


class QueryResult:
    def __init__(self, ok, query_id, event_id, id_seq, record=None, error=""):
        self.ok = ok
        self.query_id = query_id
        self.event_id = int(event_id)
        self.id_seq = list(id_seq)
        self.record = record
        self.error = error

    def to_dict(self):
        return {
            "ok": self.ok,
            "query_id": self.query_id,
            "event_id": self.event_id,
            "id_seq": self.id_seq,
            "record": self.record,
            "error": self.error,
        }


def peer_tuple_from_info(peer):
    return PeerTuple(peer.name, peer.ip, peer.pport)


def dict_to_peer_tuple(data):
    return PeerTuple(data["name"], data["ip"], data["pport"])


def peer_tuple_to_dict(peer):
    return {"name": peer.name, "ip": peer.ip, "pport": peer.pport}


def get_my_ip_for_manager(manager_ip):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((manager_ip, 9))
        return s.getsockname()[0]
    finally:
        s.close()


def candidate_dataset_paths(year):
    base = f"details-{year}.csv"
    gz = f"{base}.gz"
    here = Path.cwd()
    script = Path(__file__).resolve().parent
    env_root = os.environ.get("STORM_DATA_DIR")

    paths = [
        here / base,
        here / gz,
        script / base,
        script / gz,
        script / "data" / base,
        script / "data" / gz,
        Path("/mnt/data") / base,
        Path("/mnt/data") / gz,
        Path("../../Downloads/434socket-main/434socket-main") / base,
        Path("../../Downloads/434socket-main/434socket-main") / gz,
        here / f"StormEvents_details-ftp_v1.0_d{year}_c*.csv.gz",
        script / f"StormEvents_details-ftp_v1.0_d{year}_c*.csv.gz",
    ]

    if env_root:
        paths += [Path(env_root) / base, Path(env_root) / gz]

    out = []
    seen = set()
    for path in paths:
        try:
            key = path.resolve()
        except Exception:
            key = path
        if key not in seen:
            seen.add(key)
            out.append(path)
    return out


def find_dataset(year):
    for path in candidate_dataset_paths(year):
        if "*" in str(path):
            matches = sorted(path.parent.glob(path.name))
            if matches:
                return matches[0]
        elif path.exists():
            return path

    for root in [Path.cwd(), Path(__file__).resolve().parent, Path("/mnt/data")]:
        try:
            for match in root.rglob(f"*{year}*.csv*"):
                name = match.name.lower()
                if name in {f"details-{year}.csv", f"details-{year}.csv.gz"}:
                    return match
                if f"d{year}" in name and ("details" in name or "stormevents" in name):
                    return match
        except Exception:
            pass

    searched = "\n  - ".join(str(p) for p in candidate_dataset_paths(year))
    raise FileNotFoundError(f"Could not find a dataset for year {year}. Tried:\n  - {searched}")


def open_dataset_text(path):
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", newline="", encoding="utf-8", errors="ignore")
    return path.open(newline="", encoding="utf-8", errors="ignore")


def load_records_for_year(year):
    path = find_dataset(year)
    records = []
    with open_dataset_text(path) as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 14:
                continue
            rec = dict(zip(FIELD_ORDER, row[:14]))
            try:
                int(rec["event_id"])
            except ValueError:
                continue
            records.append(rec)
    return records, next_prime(2 * len(records) + 1), path


class DHTManager:
    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", int(port)))
        print(f"[Manager] listening on UDP port {port}")

        self.peers = {}
        self.used_ports = set()
        self.current_ring_names = []
        self.current_year = None
        self.leader_name = None
        self.dht_ready = False
        self.waiting_for = WAIT_NONE
        self.pending_peer = None

    def trace(self, msg):
        print(f"[Manager] {msg}")

    def send(self, addr, payload):
        self.sock.sendto(json.dumps(payload, separators=(",", ":")).encode("utf-8"), addr)

    def fail(self, addr, cmd, reason):
        self.send(addr, {"cmd": cmd, "status": FAIL, "reason": reason})

    def ring_infos(self):
        return [self.peers[name] for name in self.current_ring_names]

    def ring_tuples(self):
        return [peer_tuple_to_dict(peer_tuple_from_info(p)) for p in self.ring_infos()]

    def mark_states_from_ring(self):
        ring_set = set(self.current_ring_names)
        for peer in self.peers.values():
            peer.state = STATE_INDHT if peer.name in ring_set else STATE_FREE
        if self.leader_name in self.peers and self.leader_name in ring_set:
            self.peers[self.leader_name].state = STATE_LEADER

    def choose_random_dht_peer(self):
        return self.peers[random.choice(self.current_ring_names)]

    def handle_register(self, addr, msg):
        name = str(msg.get("peer_name", "")).strip()
        ip = str(msg.get("ip", "")).strip()
        try:
            mport = int(msg.get("mport"))
            pport = int(msg.get("pport"))
        except Exception:
            self.fail(addr, "REGISTER", "bad ports")
            return

        if not name or len(name) > 15 or not name.isalpha():
            self.fail(addr, "REGISTER", "peer-name must be alphabetic and <=15 chars")
            return
        if name in self.peers:
            self.fail(addr, "REGISTER", "duplicate peer-name")
            return
        if mport <= 0 or pport <= 0:
            self.fail(addr, "REGISTER", "ports must be > 0")
            return
        if mport in self.used_ports or pport in self.used_ports:
            self.fail(addr, "REGISTER", "port already in use")
            return

        self.peers[name] = PeerInfo(name, ip, mport, pport)
        self.used_ports.update([mport, pport])
        self.trace(f"REGISTER success for {name}")
        self.send(addr, {"cmd": "REGISTER", "status": SUCCESS})

    def handle_setup(self, addr, msg):
        leader_name = str(msg.get("peer_name", ""))
        try:
            n = int(msg.get("n"))
        except Exception:
            self.fail(addr, "SETUP", "bad n")
            return
        year = str(msg.get("year", ""))

        if self.current_ring_names:
            self.fail(addr, "SETUP", "a DHT already exists")
            return
        if leader_name not in self.peers:
            self.fail(addr, "SETUP", "peer not registered")
            return
        if n < 3:
            self.fail(addr, "SETUP", "n must be at least 3")
            return
        if len(self.peers) < n:
            self.fail(addr, "SETUP", "fewer than n peers are registered")
            return
        try:
            find_dataset(year)
        except FileNotFoundError as exc:
            self.fail(addr, "SETUP", str(exc))
            return
        if self.peers[leader_name].state != STATE_FREE:
            self.fail(addr, "SETUP", "leader is not FREE")
            return

        free_others = [p for p in self.peers.values() if p.state == STATE_FREE and p.name != leader_name]
        if len(free_others) < n - 1:
            self.fail(addr, "SETUP", "not enough FREE peers")
            return

        selected = [self.peers[leader_name]] + random.sample(free_others, n - 1)
        self.current_ring_names = [p.name for p in selected]
        self.current_year = year
        self.leader_name = leader_name
        self.dht_ready = False
        self.waiting_for = WAIT_DHT_COMPLETE
        self.pending_peer = leader_name
        self.mark_states_from_ring()

        self.send(addr, {
            "cmd": "SETUP",
            "status": SUCCESS,
            "ring": self.ring_tuples(),
            "year": self.current_year,
        })

    def handle_dht_complete(self, addr, msg):
        name = str(msg.get("peer_name", ""))
        if self.waiting_for != WAIT_DHT_COMPLETE:
            self.fail(addr, "DHT_COMPLETE", "manager not waiting for dht-complete")
            return
        if name != self.leader_name:
            self.fail(addr, "DHT_COMPLETE", "peer is not the leader")
            return

        self.dht_ready = True
        self.waiting_for = WAIT_NONE
        self.pending_peer = None
        self.send(addr, {"cmd": "DHT_COMPLETE", "status": SUCCESS})

    def handle_query_dht(self, addr, msg):
        name = str(msg.get("peer_name", ""))
        if not self.dht_ready or not self.current_ring_names:
            self.fail(addr, "QUERY_DHT", "DHT has not been completed")
            return
        if name not in self.peers:
            self.fail(addr, "QUERY_DHT", "peer not registered")
            return
        if self.peers[name].state != STATE_FREE:
            self.fail(addr, "QUERY_DHT", "peer must be FREE to query")
            return

        start = self.choose_random_dht_peer()
        self.send(addr, {
            "cmd": "QUERY_DHT",
            "status": SUCCESS,
            "start_peer": peer_tuple_to_dict(peer_tuple_from_info(start)),
            "year": self.current_year,
        })

    def handle_leave_dht(self, addr, msg):
        name = str(msg.get("peer_name", ""))
        if not self.dht_ready or not self.current_ring_names:
            self.fail(addr, "LEAVE_DHT", "DHT does not exist")
            return
        if name not in self.current_ring_names:
            self.fail(addr, "LEAVE_DHT", "peer is not maintaining the DHT")
            return

        self.waiting_for = WAIT_DHT_REBUILT
        self.pending_peer = name
        self.dht_ready = False
        self.send(addr, {
            "cmd": "LEAVE_DHT",
            "status": SUCCESS,
            "ring": self.ring_tuples(),
            "year": self.current_year,
        })

    def handle_join_dht(self, addr, msg):
        name = str(msg.get("peer_name", ""))
        if not self.dht_ready or not self.current_ring_names:
            self.fail(addr, "JOIN_DHT", "DHT does not exist")
            return
        if name not in self.peers:
            self.fail(addr, "JOIN_DHT", "peer not registered")
            return
        if self.peers[name].state != STATE_FREE:
            self.fail(addr, "JOIN_DHT", "peer is not FREE")
            return

        self.waiting_for = WAIT_DHT_REBUILT
        self.pending_peer = name
        self.dht_ready = False
        leader = self.peers[self.leader_name or self.current_ring_names[0]]
        self.send(addr, {
            "cmd": "JOIN_DHT",
            "status": SUCCESS,
            "leader": peer_tuple_to_dict(peer_tuple_from_info(leader)),
            "ring": self.ring_tuples(),
            "year": self.current_year,
        })

    def handle_dht_rebuilt(self, addr, msg):
        name = str(msg.get("peer_name", ""))
        new_leader = str(msg.get("new_leader", ""))
        ring_names = [str(x) for x in msg.get("ring_names", [])]
        year = str(msg.get("year", self.current_year or ""))

        if self.waiting_for != WAIT_DHT_REBUILT:
            self.fail(addr, "DHT_REBUILT", "manager not waiting for dht-rebuilt")
            return
        if name != self.pending_peer:
            self.fail(addr, "DHT_REBUILT", "peer is not the join/leave initiator")
            return
        if not ring_names:
            self.fail(addr, "DHT_REBUILT", "missing rebuilt ring")
            return
        if new_leader not in ring_names:
            self.fail(addr, "DHT_REBUILT", "new leader is not in rebuilt ring")
            return

        self.current_ring_names = ring_names
        self.current_year = year
        self.leader_name = new_leader
        self.mark_states_from_ring()
        self.dht_ready = True
        self.waiting_for = WAIT_NONE
        self.pending_peer = None
        self.send(addr, {"cmd": "DHT_REBUILT", "status": SUCCESS})

    def handle_deregister(self, addr, msg):
        name = str(msg.get("peer_name", ""))
        if name not in self.peers:
            self.fail(addr, "DEREGISTER", "peer not registered")
            return
        peer = self.peers[name]
        if peer.state != STATE_FREE:
            self.fail(addr, "DEREGISTER", "peer is not FREE")
            return

        self.used_ports.discard(peer.mport)
        self.used_ports.discard(peer.pport)
        del self.peers[name]
        self.send(addr, {"cmd": "DEREGISTER", "status": SUCCESS})

    def handle_teardown_dht(self, addr, msg):
        name = str(msg.get("peer_name", ""))
        if not self.dht_ready or not self.current_ring_names:
            self.fail(addr, "TEARDOWN_DHT", "DHT does not exist")
            return
        if name != self.leader_name:
            self.fail(addr, "TEARDOWN_DHT", "peer is not the leader")
            return

        self.waiting_for = WAIT_TEARDOWN_COMPLETE
        self.pending_peer = name
        self.dht_ready = False
        self.send(addr, {"cmd": "TEARDOWN_DHT", "status": SUCCESS})

    def handle_teardown_complete(self, addr, msg):
        name = str(msg.get("peer_name", ""))
        if self.waiting_for != WAIT_TEARDOWN_COMPLETE:
            self.fail(addr, "TEARDOWN_COMPLETE", "manager not waiting for teardown-complete")
            return
        if name != self.leader_name:
            self.fail(addr, "TEARDOWN_COMPLETE", "peer is not the leader")
            return

        for ring_name in self.current_ring_names:
            if ring_name in self.peers:
                self.peers[ring_name].state = STATE_FREE
        self.current_ring_names = []
        self.current_year = None
        self.leader_name = None
        self.dht_ready = False
        self.waiting_for = WAIT_NONE
        self.pending_peer = None
        self.send(addr, {"cmd": "TEARDOWN_COMPLETE", "status": SUCCESS})

    def handle_message(self, payload, addr):
        cmd = str(payload.get("cmd", "")).upper()

        if self.waiting_for == WAIT_DHT_COMPLETE and cmd != "DHT_COMPLETE":
            self.fail(addr, cmd, "manager waiting for dht-complete")
            return
        if self.waiting_for == WAIT_DHT_REBUILT and cmd != "DHT_REBUILT":
            self.fail(addr, cmd, "manager waiting for dht-rebuilt")
            return
        if self.waiting_for == WAIT_TEARDOWN_COMPLETE and cmd != "TEARDOWN_COMPLETE":
            self.fail(addr, cmd, "manager waiting for teardown-complete")
            return

        handlers = {
            "REGISTER": self.handle_register,
            "SETUP": self.handle_setup,
            "DHT_COMPLETE": self.handle_dht_complete,
            "QUERY_DHT": self.handle_query_dht,
            "LEAVE_DHT": self.handle_leave_dht,
            "JOIN_DHT": self.handle_join_dht,
            "DHT_REBUILT": self.handle_dht_rebuilt,
            "DEREGISTER": self.handle_deregister,
            "TEARDOWN_DHT": self.handle_teardown_dht,
            "TEARDOWN_COMPLETE": self.handle_teardown_complete,
        }
        handler = handlers.get(cmd)
        if handler is None:
            self.fail(addr, cmd or "UNKNOWN", "unknown command")
        else:
            handler(addr, payload)

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(65535)
            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception:
                self.fail(addr, "UNKNOWN", "invalid JSON message")
                continue
            self.handle_message(payload, addr)


class DHTPeer:
    def __init__(self, manager_host, manager_port, m_port, p_port, my_name):
        self.name = my_name
        self.manager_addr = (manager_host, int(manager_port))
        self.m_port = int(m_port)
        self.p_port = int(p_port)

        self.m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.m_sock.bind(("0.0.0.0", self.m_port))
        self.m_sock.settimeout(10.0)

        self.p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.p_sock.bind(("0.0.0.0", self.p_port))

        self.my_id = -1
        self.ring_size = 0
        self.ring = []
        self.right_neighbor = None
        self.current_year = None
        self.hash_size = 0
        self.local_hashtable = {}
        self.my_record_count = 0

        self.stop_event = threading.Event()
        self._setid_acks = set()
        self._setid_ack_lock = threading.Lock()
        self._count_done = threading.Event()
        self._counts = {}
        self._query_lock = threading.Lock()
        self._pending_queries = {}
        self._teardown_complete = threading.Event()
        self._reset_complete = threading.Event()
        self._rebuild_done = threading.Event()
        self._rebuild_result = None

        threading.Thread(target=self._peer_receive_loop, daemon=True).start()
        self.trace(f"started with m-port={self.m_port} p-port={self.p_port}")

    def trace(self, msg):
        print(f"[{self.name}] {msg}")

    def send_json(self, sock, addr, payload, label):
        self.trace(f"TX {label} -> {addr}")
        sock.sendto(json.dumps(payload, separators=(",", ":")).encode("utf-8"), addr)

    def send_manager(self, payload):
        self.send_json(self.m_sock, self.manager_addr, payload, payload.get("cmd", "MGR"))
        try:
            data, _ = self.m_sock.recvfrom(65535)
            reply = json.loads(data.decode("utf-8"))
        except socket.timeout:
            reply = {
                "cmd": str(payload.get("cmd", "MGR")),
                "status": FAIL,
                "reason": f"manager {self.manager_addr[0]}:{self.manager_addr[1]} did not respond before timeout",
            }
        self.trace(f"RX manager reply: {reply}")
        return reply

    def send_peer(self, peer, payload, label=None):
        self.send_json(self.p_sock, (peer.ip, peer.pport), payload, label or payload.get("cmd", "P2P"))

    def my_tuple(self):
        return PeerTuple(self.name, get_my_ip_for_manager(self.manager_addr[0]), self.p_port)

    def update_ring(self, ring, my_id):
        self.ring = list(ring)
        self.ring_size = len(ring)
        self.my_id = my_id
        self.right_neighbor = self.ring[(my_id + 1) % self.ring_size] if self.ring_size else None

    def clear_local_hash(self):
        self.local_hashtable.clear()
        self.my_record_count = 0

    def set_dht_metadata(self, year, hash_size):
        self.current_year = year
        self.hash_size = hash_size

    def register(self):
        return self.send_manager({
            "cmd": "REGISTER",
            "peer_name": self.name,
            "ip": get_my_ip_for_manager(self.manager_addr[0]),
            "mport": self.m_port,
            "pport": self.p_port,
        })

    def deregister(self):
        return self.send_manager({"cmd": "DEREGISTER", "peer_name": self.name})

    def setup_dht(self, n, year):
        reply = self.send_manager({"cmd": "SETUP", "peer_name": self.name, "n": int(n), "year": year})
        if reply.get("status") != SUCCESS:
            return reply

        ring = [dict_to_peer_tuple(x) for x in reply["ring"]]
        self.current_year = str(reply["year"])
        if ring[0].name != self.name:
            self.trace("not the leader for setup; waiting for SET_ID")
            return reply

        self.update_ring(ring, 0)
        self._broadcast_set_id(ring)
        self._build_dht_as_leader(notify_manager=True)
        return reply

    def query_event(self, event_id):
        reply = self.send_manager({"cmd": "QUERY_DHT", "peer_name": self.name})
        if reply.get("status") != SUCCESS:
            self.trace(f"query-dht failed: {reply}")
            return None

        start_peer = dict_to_peer_tuple(reply["start_peer"])
        query_id = uuid.uuid4().hex
        done = threading.Event()
        with self._query_lock:
            self._pending_queries[query_id] = [done, None]

        self.send_peer(start_peer, {
            "cmd": "FIND_EVENT",
            "query_id": query_id,
            "event_id": int(event_id),
            "requester": peer_tuple_to_dict(self.my_tuple()),
            "id_seq": [],
            "remaining_ids": [],
        }, "FIND_EVENT")

        if not done.wait(timeout=20.0):
            self.trace(f"query {query_id} timed out")
            with self._query_lock:
                self._pending_queries.pop(query_id, None)
            return None

        with self._query_lock:
            pair = self._pending_queries.pop(query_id, None)
        result = None if pair is None else pair[1]
        if result is not None:
            self.print_query_result(result)
        return result

    def leave_dht(self):
        reply = self.send_manager({"cmd": "LEAVE_DHT", "peer_name": self.name})
        if reply.get("status") != SUCCESS:
            return reply

        ring = [dict_to_peer_tuple(x) for x in reply["ring"]]
        self.current_year = str(reply["year"])
        self._teardown_complete.clear()
        self._start_teardown(self.name)
        self._teardown_complete.wait(timeout=20.0)

        names = [p.name for p in ring]
        if self.name not in names:
            return reply
        leave_index = names.index(self.name)
        new_ring = [ring[(leave_index + i) % len(ring)] for i in range(1, len(ring))]
        if not new_ring:
            return reply

        self._reset_complete.clear()
        self.send_peer(new_ring[0], {
            "cmd": "RESET_ID",
            "origin": self.name,
            "origin_tuple": peer_tuple_to_dict(self.my_tuple()),
            "ring": [peer_tuple_to_dict(p) for p in new_ring],
            "next_id": 0,
            "year": self.current_year,
        }, "RESET_ID")
        self._reset_complete.wait(timeout=20.0)

        self.update_ring([], -1)
        self.clear_local_hash()

        self._rebuild_done.clear()
        self.send_peer(new_ring[0], {
            "cmd": "REBUILD_DHT",
            "origin": self.name,
            "ring": [peer_tuple_to_dict(p) for p in new_ring],
            "year": self.current_year,
        }, "REBUILD_DHT")
        self._rebuild_done.wait(timeout=30.0)

        return self.send_manager({
            "cmd": "DHT_REBUILT",
            "peer_name": self.name,
            "new_leader": new_ring[0].name,
            "ring_names": [p.name for p in new_ring],
            "year": self.current_year,
        })

    def join_dht(self):
        reply = self.send_manager({"cmd": "JOIN_DHT", "peer_name": self.name})
        if reply.get("status") != SUCCESS:
            return reply

        leader = dict_to_peer_tuple(reply["leader"])
        ring = [dict_to_peer_tuple(x) for x in reply["ring"]]
        self.current_year = str(reply["year"])
        self._rebuild_done.clear()
        self._rebuild_result = None

        self.send_peer(leader, {
            "cmd": "JOIN_REQUEST",
            "joiner": peer_tuple_to_dict(self.my_tuple()),
            "year": self.current_year,
            "ring": [peer_tuple_to_dict(p) for p in ring],
        }, "JOIN_REQUEST")

        self._rebuild_done.wait(timeout=30.0)
        if not self._rebuild_result:
            self.trace("join rebuild did not finish in time")
            return reply

        return self.send_manager({
            "cmd": "DHT_REBUILT",
            "peer_name": self.name,
            "new_leader": self._rebuild_result["new_leader"],
            "ring_names": self._rebuild_result["ring_names"],
            "year": self.current_year,
        })

    def teardown_dht(self):
        reply = self.send_manager({"cmd": "TEARDOWN_DHT", "peer_name": self.name})
        if reply.get("status") != SUCCESS:
            return reply

        self._teardown_complete.clear()
        self._start_teardown(self.name)
        self._teardown_complete.wait(timeout=20.0)
        manager_reply = self.send_manager({"cmd": "TEARDOWN_COMPLETE", "peer_name": self.name})
        if manager_reply.get("status") == SUCCESS:
            self.update_ring([], -1)
            self.clear_local_hash()
            self.current_year = None
        return manager_reply

    def _broadcast_set_id(self, ring):
        with self._setid_ack_lock:
            self._setid_acks.clear()

        for i in range(1, len(ring)):
            self.send_peer(ring[i], {
                "cmd": "SET_ID",
                "id": i,
                "ring": [peer_tuple_to_dict(p) for p in ring],
                "year": self.current_year,
            }, "SET_ID")

        deadline = time.time() + 10.0
        while time.time() < deadline:
            with self._setid_ack_lock:
                if len(self._setid_acks) >= len(ring) - 1:
                    return
            time.sleep(0.05)

    def _build_dht_as_leader(self, notify_manager=True):
        if self.my_id != 0:
            raise RuntimeError("Only the leader can build the DHT")

        records, hash_size, dataset_path = load_records_for_year(str(self.current_year))
        self.clear_local_hash()
        self.set_dht_metadata(str(self.current_year), hash_size)
        self.trace(f"loading dataset {dataset_path} records={len(records)} hash_size={hash_size}")

        for rec in records:
            event_id = int(rec["event_id"])
            pos = event_id % hash_size
            target_id = pos % self.ring_size
            if target_id == self.my_id:
                self.local_hashtable.setdefault(pos, []).append(rec)
                self.my_record_count += 1
            elif self.right_neighbor is not None:
                self.send_peer(self.right_neighbor, {
                    "cmd": "STORE",
                    "pos": pos,
                    "target_id": target_id,
                    "record": rec,
                    "hash_size": hash_size,
                    "year": self.current_year,
                }, "STORE")

        time.sleep(1.0)
        self._counts = {0: self.my_record_count}
        self._count_done.clear()
        if self.ring_size > 1 and self.right_neighbor is not None:
            self.send_peer(self.right_neighbor, {
                "cmd": "COUNT_TOKEN",
                "origin": self.name,
                "counts": {"0": self.my_record_count},
            }, "COUNT_TOKEN")
            self._count_done.wait(timeout=10.0)
        else:
            self._count_done.set()

        self.print_ring_counts()
        if notify_manager:
            self.trace(f"DHT_COMPLETE -> {self.send_manager({'cmd': 'DHT_COMPLETE', 'peer_name': self.name})}")

    def print_ring_counts(self):
        self.trace("=" * 60)
        self.trace("DHT setup/rebuild complete – record counts per node:")
        total = 0
        for i, peer in enumerate(self.ring):
            cnt = int(self._counts.get(i, 0))
            total += cnt
            self.trace(f"  node {i} ({peer.name}): {cnt} records")
        self.trace(f"Total records counted: {total}")
        self.trace("=" * 60)

    def _start_teardown(self, origin_name):
        if not self.ring or self.right_neighbor is None:
            self.clear_local_hash()
            self._teardown_complete.set()
            return
        self.send_peer(self.right_neighbor, {"cmd": "TEARDOWN", "origin": origin_name}, "TEARDOWN")

    def _peer_receive_loop(self):
        while not self.stop_event.is_set():
            try:
                data, addr = self.p_sock.recvfrom(65535)
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            self.trace(f"RX from {addr}: {msg}")
            handler = getattr(self, f"_on_{str(msg.get('cmd', '')).lower()}", None)
            if handler is not None:
                handler(msg, addr)

    def _on_set_id(self, msg, addr):
        ring = [dict_to_peer_tuple(x) for x in msg["ring"]]
        self.current_year = str(msg["year"])
        self.update_ring(ring, int(msg["id"]))
        self.send_peer(ring[0], {"cmd": "SET_ID_ACK", "id": self.my_id, "peer_name": self.name}, "SET_ID_ACK")

    def _on_set_id_ack(self, msg, addr):
        with self._setid_ack_lock:
            self._setid_acks.add(int(msg["id"]))

    def _on_store(self, msg, addr):
        pos = int(msg["pos"])
        target_id = int(msg["target_id"])
        self.hash_size = int(msg["hash_size"])
        self.current_year = str(msg["year"])
        if target_id == self.my_id:
            self.local_hashtable.setdefault(pos, []).append(dict(msg["record"]))
            self.my_record_count += 1
        elif self.right_neighbor is not None:
            self.send_peer(self.right_neighbor, msg, "STORE")

    def _on_count_token(self, msg, addr):
        counts = {int(k): int(v) for k, v in msg.get("counts", {}).items()}
        origin = str(msg["origin"])
        if self.name == origin:
            self._counts = counts
            self._count_done.set()
            return
        counts[self.my_id] = self.my_record_count
        if self.right_neighbor is not None:
            self.send_peer(self.right_neighbor, {
                "cmd": "COUNT_TOKEN",
                "origin": origin,
                "counts": {str(k): v for k, v in counts.items()},
            }, "COUNT_TOKEN")

    def _find_local_record(self, pos, event_id):
        for rec in self.local_hashtable.get(pos, []):
            try:
                if int(rec["event_id"]) == event_id:
                    return rec
            except Exception:
                pass
        return None

    def _on_find_event(self, msg, addr):
        query_id = str(msg["query_id"])
        event_id = int(msg["event_id"])
        requester = dict_to_peer_tuple(msg["requester"])
        id_seq = [int(x) for x in msg.get("id_seq", [])]
        remaining_ids = [int(x) for x in msg.get("remaining_ids", [])]

        if self.hash_size <= 0 or self.ring_size <= 0:
            result = QueryResult(False, query_id, event_id, id_seq, None, "DHT metadata unavailable")
            self.send_peer(requester, {"cmd": "FIND_RESULT", **result.to_dict()}, "FIND_RESULT")
            return

        if not id_seq:
            id_seq = [self.my_id]
            remaining_ids = [i for i in range(self.ring_size) if i != self.my_id]
        elif id_seq[-1] != self.my_id:
            id_seq.append(self.my_id)
            remaining_ids = [i for i in remaining_ids if i != self.my_id]

        pos = event_id % self.hash_size
        record = self._find_local_record(pos, event_id)
        if record is not None:
            result = QueryResult(True, query_id, event_id, id_seq, record, "")
            self.send_peer(requester, {"cmd": "FIND_RESULT", **result.to_dict()}, "FIND_RESULT")
            return

        if not remaining_ids:
            result = QueryResult(False, query_id, event_id, id_seq, None, "not found")
            self.send_peer(requester, {"cmd": "FIND_RESULT", **result.to_dict()}, "FIND_RESULT")
            return

        next_id = random.choice(remaining_ids)
        remaining_ids = [i for i in remaining_ids if i != next_id]
        self.send_peer(self.ring[next_id], {
            "cmd": "FIND_EVENT",
            "query_id": query_id,
            "event_id": event_id,
            "requester": peer_tuple_to_dict(requester),
            "id_seq": id_seq,
            "remaining_ids": remaining_ids,
        }, "FIND_EVENT")

    def _on_find_result(self, msg, addr):
        result = QueryResult(
            bool(msg["ok"]),
            str(msg["query_id"]),
            int(msg["event_id"]),
            [int(x) for x in msg.get("id_seq", [])],
            msg.get("record"),
            str(msg.get("error", "")),
        )
        with self._query_lock:
            pair = self._pending_queries.get(result.query_id)
            if pair is None:
                return
            pair[1] = result
            pair[0].set()

    def _on_teardown(self, msg, addr):
        origin = str(msg["origin"])
        if self.name == origin:
            self.clear_local_hash()
            self._teardown_complete.set()
            return
        self.clear_local_hash()
        if self.right_neighbor is not None:
            self.send_peer(self.right_neighbor, msg, "TEARDOWN")

    def _on_reset_id(self, msg, addr):
        origin_tuple = dict_to_peer_tuple(msg["origin_tuple"])
        ring = [dict_to_peer_tuple(x) for x in msg["ring"]]
        self.current_year = str(msg["year"])
        next_id = int(msg["next_id"])
        self.update_ring(ring, next_id)

        if next_id + 1 >= len(ring):
            self.send_peer(origin_tuple, {"cmd": "RESET_DONE", "origin": msg["origin"]}, "RESET_DONE")
            return

        if self.right_neighbor is not None:
            self.send_peer(self.right_neighbor, {
                "cmd": "RESET_ID",
                "origin": msg["origin"],
                "origin_tuple": peer_tuple_to_dict(origin_tuple),
                "ring": [peer_tuple_to_dict(p) for p in ring],
                "next_id": next_id + 1,
                "year": self.current_year,
            }, "RESET_ID")

    def _on_reset_done(self, msg, addr):
        self._reset_complete.set()

    def _on_rebuild_dht(self, msg, addr):
        ring = [dict_to_peer_tuple(x) for x in msg["ring"]]
        self.current_year = str(msg["year"])
        if not ring or ring[0].name != self.name:
            return

        self.update_ring(ring, 0)
        self._broadcast_set_id(ring)
        self._build_dht_as_leader(notify_manager=False)

        origin = str(msg["origin"])
        origin_peer = next((p for p in ring if p.name == origin), None)
        if origin_peer is None:
            origin_peer = PeerTuple(origin, addr[0], addr[1])
        self.send_peer(origin_peer, {
            "cmd": "REBUILD_DONE",
            "origin": origin,
            "new_leader": self.name,
            "ring_names": [p.name for p in ring],
            "year": self.current_year,
        }, "REBUILD_DONE")

    def _on_rebuild_done(self, msg, addr):
        self._rebuild_result = {
            "new_leader": str(msg["new_leader"]),
            "ring_names": list(msg["ring_names"]),
            "year": str(msg.get("year", self.current_year or "")),
        }
        self.current_year = self._rebuild_result["year"]
        self._rebuild_done.set()

    def _on_join_request(self, msg, addr):
        if self.my_id != 0 or not self.ring:
            return

        joiner = dict_to_peer_tuple(msg["joiner"])
        self.current_year = str(msg["year"])
        self._teardown_complete.clear()
        self._start_teardown(self.name)
        self._teardown_complete.wait(timeout=20.0)

        new_ring = list(self.ring) + [joiner]
        self.update_ring(new_ring, 0)
        self.send_peer(joiner, {
            "cmd": "SET_ID",
            "id": len(new_ring) - 1,
            "ring": [peer_tuple_to_dict(p) for p in new_ring],
            "year": self.current_year,
        }, "SET_ID")
        time.sleep(0.3)

        self._broadcast_set_id(new_ring)
        self._build_dht_as_leader(notify_manager=False)
        self.send_peer(joiner, {
            "cmd": "REBUILD_DONE",
            "origin": joiner.name,
            "new_leader": self.name,
            "ring_names": [p.name for p in new_ring],
            "year": self.current_year,
        }, "REBUILD_DONE")

    def print_query_result(self, result):
        if not result.ok or not result.record:
            self.trace(f"Storm event {result.event_id} not found in the DHT.")
            if result.id_seq:
                self.trace(f"id-seq: {' -> '.join(map(str, result.id_seq))}")
            return

        self.trace(f"query SUCCESS for event_id={result.event_id}")
        for field in FIELD_ORDER:
            self.trace(f"  {FIELD_LABELS[field]}: {result.record.get(field, '')}")
        self.trace(f"  id-seq: {' -> '.join(map(str, result.id_seq))}")

    def repl(self):
        self.trace("commands: setup <n> <year>, query <event_id>, leave, join, teardown, deregister, quit")
        while True:
            try:
                line = input(f"[{self.name}]> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                line = "quit"
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].lower()
            try:
                if cmd == "setup" and len(parts) == 3:
                    self.setup_dht(parts[1], parts[2])
                elif cmd == "query" and len(parts) == 2:
                    self.query_event(parts[1])
                elif cmd == "leave":
                    self.leave_dht()
                elif cmd == "join":
                    self.join_dht()
                elif cmd == "teardown":
                    self.teardown_dht()
                elif cmd == "deregister":
                    reply = self.deregister()
                    if reply.get("status") == SUCCESS:
                        self.trace("deregistered successfully; exiting")
                        break
                elif cmd in {"quit", "exit", "q"}:
                    break
                else:
                    self.trace("unknown command")
            except Exception as exc:
                self.trace(f"command failed: {exc}")

        self.stop_event.set()
        for sock in [self.p_sock, self.m_sock]:
            try:
                sock.close()
            except Exception:
                pass


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python dht_slim_no_types.py manager <manager_port>")
        print("  python dht_slim_no_types.py peer <manager_host> <manager_port> <m_port> <p_port> <peer_name>")
        sys.exit(1)

    mode = sys.argv[1].lower()
    if mode == "manager":
        if len(sys.argv) != 3:
            print("Usage: python dht_slim_no_types.py manager <manager_port>")
            sys.exit(1)
        DHTManager(sys.argv[2]).run()
    elif mode == "peer":
        if len(sys.argv) != 7:
            print("Usage: python dht_slim_no_types.py peer <manager_host> <manager_port> <m_port> <p_port> <peer_name>")
            sys.exit(1)
        peer = DHTPeer(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
        reply = peer.register()
        if reply.get("status") != SUCCESS:
            print(f"[{peer.name}] register failed: {reply}")
            sys.exit(1)
        peer.repl()
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)


if __name__ == "__main__":
    main()
