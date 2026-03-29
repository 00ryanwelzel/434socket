#!/usr/bin/env python3
from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    "event_id",
    "state",
    "year",
    "month",
    "event_type",
    "cz_type",
    "cz_name",
    "inj_direct",
    "inj_indirect",
    "death_direct",
    "death_indirect",
    "damage_property",
    "damage_crops",
    "tor_f_scale",
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


"""
    --- Helpers and dataclasses below ---
"""


def next_prime(x: int) -> int:
    """Return the first prime number greater than or equal to x."""
    if x <= 2:
        return 2

    def is_prime(n2: int) -> bool:
        """Check whether a candidate integer is prime."""
        if n2 % 2 == 0:
            return n2 == 2
        r = int(math.isqrt(n2))
        for i in range(3, r + 1, 2):
            if n2 % i == 0:
                return False
        return True

    n = x if x % 2 == 1 else x + 1
    while not is_prime(n):
        n += 2
    return n


@dataclass
class PeerTuple:
    name: str
    ip: str
    pport: int


@dataclass
class PeerInfo:
    name: str
    ip: str
    mport: int
    pport: int
    state: str = STATE_FREE


@dataclass
class QueryResult:
    ok: bool
    query_id: str
    event_id: int
    id_seq: List[int]
    record: Optional[Dict[str, str]] = None
    error: str = ""


def peer_tuple_from_info(peer: PeerInfo) -> PeerTuple:
    """Convert manager-side peer metadata into a peer-to-peer tuple."""
    return PeerTuple(peer.name, peer.ip, peer.pport)


def dict_to_peer_tuple(data: Dict[str, Any]) -> PeerTuple:
    """Rebuild a PeerTuple from a decoded message dictionary."""
    return PeerTuple(name=data["name"], ip=data["ip"], pport=int(data["pport"]))


def peer_tuple_to_dict(peer: PeerTuple) -> Dict[str, Any]:
    """Serialize a PeerTuple into a JSON-friendly dictionary."""
    return {"name": peer.name, "ip": peer.ip, "pport": peer.pport}


def get_my_ip_for_manager(manager_ip: str) -> str:
    """Infer the local IPv4 address that reaches the manager host."""
    tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        tmp.connect((manager_ip, 9))
        return tmp.getsockname()[0]
    finally:
        tmp.close()


def candidate_dataset_paths(year: str) -> List[Path]:
    """Build the list of dataset locations to probe for a given year."""
    filename = f"details-{year}.csv"
    gz_filename = f"{filename}.gz"
    here = Path.cwd()
    script_dir = Path(__file__).resolve().parent
    env_root = os.environ.get("STORM_DATA_DIR")

    candidates = [
        here / filename,
        here / gz_filename,
        script_dir / filename,
        script_dir / gz_filename,
        script_dir / "data" / filename,
        script_dir / "data" / gz_filename,
        Path("/mnt/data") / filename,
        Path("/mnt/data") / gz_filename,
    ]

    if env_root:
        candidates.append(Path(env_root) / filename)
        candidates.append(Path(env_root) / gz_filename)

    legacy = Path("../../Downloads/434socket-main/434socket-main")
    candidates.append(legacy / filename)
    candidates.append(legacy / gz_filename)
    candidates.append(here / f"StormEvents_details-ftp_v1.0_d{year}_c*.csv.gz")
    candidates.append(script_dir / f"StormEvents_details-ftp_v1.0_d{year}_c*.csv.gz")

    seen: set[Path] = set()
    deduped: List[Path] = []
    for path in candidates:
        try:
            rp = path.resolve()
        except Exception:
            rp = path
        if rp not in seen:
            seen.add(rp)
            deduped.append(path)
    return deduped


def find_dataset(year: str) -> Path:
    """Locate the storm dataset file for the requested year."""
    preferred_plain = [
        Path.cwd() / f"details-{year}.csv",
        Path(__file__).resolve().parent / f"details-{year}.csv",
        Path(__file__).resolve().parent / "data" / f"details-{year}.csv",
    ]
    for path in preferred_plain:
        if path.exists():
            return path

    for path in candidate_dataset_paths(year):
        if "*" in str(path):
            matches = sorted(path.parent.glob(path.name))
            if matches:
                return matches[0]
        elif path.exists():
            return path
    search_roots = [Path.cwd(), Path(__file__).resolve().parent, Path("/mnt/data")]
    for root in search_roots:
        try:
            for match in root.rglob(f"*{year}*.csv*"):
                lower_name = match.name.lower()
                if lower_name == f"details-{year}.csv" or lower_name == f"details-{year}.csv.gz":
                    return match
                if f"d{year}" in lower_name and ("details" in lower_name or "stormevents" in lower_name):
                    return match
        except Exception:
            pass
    searched = "\n  - ".join(str(p) for p in candidate_dataset_paths(year))
    raise FileNotFoundError(f"Could not find a dataset for year {year}. Tried:\n  - {searched}")


def open_dataset_text(path: Path):
    """Open a dataset as text, transparently handling gzip input."""
    if path.suffix.lower() == ".gz":
        return gzip.open(path, mode="rt", newline="", encoding="utf-8", errors="ignore")
    return path.open(newline="", encoding="utf-8", errors="ignore")


def load_records_for_year(year: str) -> Tuple[List[Dict[str, str]], int, Path]:
    """Load, normalize, and size the storm dataset for a given year."""
    path = find_dataset(year)
    records: List[Dict[str, str]] = []
    with open_dataset_text(path) as f:
        reader = csv.reader(f)
        _ = next(reader, None)
        for row in reader:
            if len(row) < 14:
                continue
            rec = {
                "event_id": row[0],
                "state": row[1],
                "year": row[2],
                "month": row[3],
                "event_type": row[4],
                "cz_type": row[5],
                "cz_name": row[6],
                "inj_direct": row[7],
                "inj_indirect": row[8],
                "death_direct": row[9],
                "death_indirect": row[10],
                "damage_property": row[11],
                "damage_crops": row[12],
                "tor_f_scale": row[13],
            }
            try:
                int(rec["event_id"])
            except ValueError:
                continue
            records.append(rec)
    hash_size = next_prime(2 * len(records) + 1)
    return records, hash_size, path


"""
    --- Manager class below ---
"""

class DHTManager:
    def __init__(self, port: int):
        """Initialize the always-on UDP manager and its state tables."""
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        print(f"[Manager] listening on UDP port {port}")

        self.peers: Dict[str, PeerInfo] = {}
        self.used_ports: set[int] = set()

        self.current_ring_names: List[str] = []
        self.current_year: Optional[str] = None
        self.leader_name: Optional[str] = None
        self.dht_ready: bool = False

        self.waiting_for: str = WAIT_NONE
        self.pending_peer: Optional[str] = None
        self.pending_action: Optional[str] = None

    def trace(self, msg: str) -> None:
        """Print a manager-scoped log message."""
        print(f"[Manager] {msg}")

    def send_to(self, addr: Tuple[str, int], payload: Dict[str, Any]) -> None:
        """Send a JSON reply to a peer or requester address."""
        wire = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.sock.sendto(wire, addr)

    def peer_addr(self, name: str) -> Tuple[str, int]:
        """Return the manager-facing socket address for a named peer."""
        p = self.peers[name]
        return p.ip, p.mport

    def ring_infos(self) -> List[PeerInfo]:
        """Return manager-side PeerInfo entries for the active ring."""
        return [self.peers[name] for name in self.current_ring_names]

    def ring_tuples(self) -> List[Dict[str, Any]]:
        """Return the active ring as serializable peer tuples."""
        return [peer_tuple_to_dict(peer_tuple_from_info(p)) for p in self.ring_infos()]

    def mark_states_from_ring(self) -> None:
        """Update every registered peer's state from the current ring."""
        ring_set = set(self.current_ring_names)
        for peer in self.peers.values():
            if peer.name in ring_set:
                peer.state = STATE_INDHT
            else:
                peer.state = STATE_FREE
        if self.leader_name and self.leader_name in self.peers and self.leader_name in ring_set:
            self.peers[self.leader_name].state = STATE_LEADER

    def fail(self, addr: Tuple[str, int], cmd: str, reason: str) -> None:
        """Send a standardized failure reply for a command."""
        self.send_to(addr, {"cmd": cmd, "status": FAIL, "reason": reason})

    def require_wait_state(self, addr: Tuple[str, int], cmd: str, allowed_cmd: str) -> bool:
        """Reject commands while the manager is waiting for a specific follow-up."""
        if self.waiting_for != WAIT_NONE and cmd != allowed_cmd:
            self.fail(addr, cmd, f"manager waiting for {allowed_cmd}")
            return False
        return True

    def choose_random_dht_peer(self) -> PeerInfo:
        """Pick a random peer from the active DHT ring."""
        return self.peers[random.choice(self.current_ring_names)]

    def handle_register(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Register a peer with the manager after validating uniqueness rules."""
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

        self.peers[name] = PeerInfo(name=name, ip=ip, mport=mport, pport=pport, state=STATE_FREE)
        self.used_ports.add(mport)
        self.used_ports.add(pport)
        self.trace(f"REGISTER success for {name} ip={ip} mport={mport} pport={pport}")
        self.send_to(addr, {"cmd": "REGISTER", "status": SUCCESS})

    def handle_setup(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Validate and begin construction of a new DHT ring."""
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
        leader = self.peers[leader_name]
        if leader.state != STATE_FREE:
            self.fail(addr, "SETUP", "leader is not FREE")
            return

        free_others = [p for p in self.peers.values() if p.state == STATE_FREE and p.name != leader_name]
        if len(free_others) < n - 1:
            self.fail(addr, "SETUP", "not enough FREE peers")
            return

        selected = [leader] + random.sample(free_others, n - 1)
        self.current_ring_names = [p.name for p in selected]
        self.current_year = year
        self.leader_name = leader_name
        self.dht_ready = False
        self.waiting_for = WAIT_DHT_COMPLETE
        self.pending_peer = leader_name
        self.pending_action = "setup"
        self.mark_states_from_ring()

        self.trace(f"SETUP accepted leader={leader_name} n={n} year={year} ring={self.current_ring_names}")
        self.send_to(
            addr,
            {
                "cmd": "SETUP",
                "status": SUCCESS,
                "ring": self.ring_tuples(),
                "year": self.current_year,
            },
        )

    def handle_dht_complete(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Mark the DHT as ready after the leader completes setup."""
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
        self.pending_action = None
        self.trace(f"DHT_COMPLETE from {name}; DHT is ready")
        self.send_to(addr, {"cmd": "DHT_COMPLETE", "status": SUCCESS})

    def handle_query_dht(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Authorize a query and return a random DHT start node."""
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
        self.trace(f"QUERY_DHT from {name}; start node chosen={start.name}")
        self.send_to(
            addr,
            {
                "cmd": "QUERY_DHT",
                "status": SUCCESS,
                "start_peer": peer_tuple_to_dict(peer_tuple_from_info(start)),
                "year": self.current_year,
            },
        )

    def handle_leave_dht(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Begin the leave-and-rebuild workflow for a DHT member."""
        name = str(msg.get("peer_name", ""))
        if not self.dht_ready or not self.current_ring_names:
            self.fail(addr, "LEAVE_DHT", "DHT does not exist")
            return
        if name not in self.current_ring_names:
            self.fail(addr, "LEAVE_DHT", "peer is not maintaining the DHT")
            return

        self.waiting_for = WAIT_DHT_REBUILT
        self.pending_peer = name
        self.pending_action = "leave"
        self.dht_ready = False

        self.trace(f"LEAVE_DHT accepted from {name}")
        self.send_to(
            addr,
            {
                "cmd": "LEAVE_DHT",
                "status": SUCCESS,
                "ring": self.ring_tuples(),
                "year": self.current_year,
            },
        )

    def handle_join_dht(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Begin the join-and-rebuild workflow for a free peer."""
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
        self.pending_action = "join"
        self.dht_ready = False

        leader = self.peers[self.leader_name] if self.leader_name else self.peers[self.current_ring_names[0]]
        self.trace(f"JOIN_DHT accepted from {name}")
        self.send_to(
            addr,
            {
                "cmd": "JOIN_DHT",
                "status": SUCCESS,
                "leader": peer_tuple_to_dict(peer_tuple_from_info(leader)),
                "ring": self.ring_tuples(),
                "year": self.current_year,
            },
        )

    def handle_dht_rebuilt(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Accept a rebuilt ring after a join or leave operation completes."""
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
        self.pending_action = None

        self.trace(f"DHT_REBUILT from {name}; leader={new_leader}; ring={ring_names}")
        self.send_to(addr, {"cmd": "DHT_REBUILT", "status": SUCCESS})

    def handle_deregister(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Remove a free peer from the manager's registration table."""
        name = str(msg.get("peer_name", ""))
        if name not in self.peers:
            self.fail(addr, "DEREGISTER", "peer not registered")
            return
        peer = self.peers[name]
        if peer.state != STATE_FREE:
            self.fail(addr, "DEREGISTER", "peer is not FREE")
            return

        self.trace(f"DEREGISTER success for {name}")
        self.used_ports.discard(peer.mport)
        self.used_ports.discard(peer.pport)
        del self.peers[name]
        self.send_to(addr, {"cmd": "DEREGISTER", "status": SUCCESS})

    def handle_teardown_dht(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Begin teardown of the current DHT when requested by the leader."""
        name = str(msg.get("peer_name", ""))
        if not self.dht_ready or not self.current_ring_names:
            self.fail(addr, "TEARDOWN_DHT", "DHT does not exist")
            return
        if name != self.leader_name:
            self.fail(addr, "TEARDOWN_DHT", "peer is not the leader")
            return

        self.waiting_for = WAIT_TEARDOWN_COMPLETE
        self.pending_peer = name
        self.pending_action = "teardown"
        self.dht_ready = False
        self.trace(f"TEARDOWN_DHT accepted from leader {name}")
        self.send_to(addr, {"cmd": "TEARDOWN_DHT", "status": SUCCESS})

    def handle_teardown_complete(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> None:
        """Clear the current ring after the leader confirms teardown finished."""
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
        self.trace(f"TEARDOWN_COMPLETE from {name}; ring cleared")
        self.current_ring_names = []
        self.current_year = None
        self.leader_name = None
        self.dht_ready = False
        self.waiting_for = WAIT_NONE
        self.pending_peer = None
        self.pending_action = None
        self.send_to(addr, {"cmd": "TEARDOWN_COMPLETE", "status": SUCCESS})

    def handle_message(self, payload: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Route one incoming manager message to its command handler."""
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

        if cmd == "REGISTER":
            self.handle_register(addr, payload)
        elif cmd == "SETUP":
            self.handle_setup(addr, payload)
        elif cmd == "DHT_COMPLETE":
            self.handle_dht_complete(addr, payload)
        elif cmd == "QUERY_DHT":
            self.handle_query_dht(addr, payload)
        elif cmd == "LEAVE_DHT":
            self.handle_leave_dht(addr, payload)
        elif cmd == "JOIN_DHT":
            self.handle_join_dht(addr, payload)
        elif cmd == "DHT_REBUILT":
            self.handle_dht_rebuilt(addr, payload)
        elif cmd == "DEREGISTER":
            self.handle_deregister(addr, payload)
        elif cmd == "TEARDOWN_DHT":
            self.handle_teardown_dht(addr, payload)
        elif cmd == "TEARDOWN_COMPLETE":
            self.handle_teardown_complete(addr, payload)
        else:
            self.fail(addr, cmd or "UNKNOWN", "unknown command")

    def run(self) -> None:
        """Serve manager requests forever on the bound UDP socket."""
        while True:
            data, addr = self.sock.recvfrom(65535)
            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception:
                self.fail(addr, "UNKNOWN", "invalid JSON message")
                continue
            self.handle_message(payload, addr)


"""
    --- Peer class below ---
"""


class DHTPeer:
    def __init__(self, manager_host: str, manager_port: int, m_port: int, p_port: int, my_name: str):
        """Initialize peer sockets, DHT state, and background receive loop."""
        self.name = my_name
        self.manager_addr = (manager_host, manager_port)
        self.m_port = m_port
        self.p_port = p_port

        self.m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.m_sock.bind(("0.0.0.0", m_port))
        self.m_sock.settimeout(10.0)

        self.p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.p_sock.bind(("0.0.0.0", p_port))

        self.trace(f"started with m-port={m_port} p-port={p_port}")

        self.state_lock = threading.Lock()
        self.stop_event = threading.Event()

        self.my_id: int = -1
        self.ring_size: int = 0
        self.ring: List[PeerTuple] = []
        self.right_neighbor: Optional[PeerTuple] = None
        self.current_year: Optional[str] = None
        self.hash_size: int = 0
        self.local_hashtable: Dict[int, List[Dict[str, str]]] = {}
        self.my_record_count: int = 0

        self._setid_ack_lock = threading.Lock()
        self._setid_acks: set[int] = set()
        self._count_done = threading.Event()
        self._counts: Dict[int, int] = {}
        self._query_lock = threading.Lock()
        self._pending_queries: Dict[str, Tuple[threading.Event, Optional[QueryResult]]] = {}
        self._teardown_complete = threading.Event()
        self._reset_complete = threading.Event()
        self._rebuild_done = threading.Event()
        self._rebuild_result: Optional[Dict[str, Any]] = None
        self._join_setid_done = threading.Event()
        self._shutdown_allowed = False

        threading.Thread(target=self._peer_receive_loop, daemon=True).start()

    def trace(self, msg: str) -> None:
        """Print a peer-scoped log message."""
        print(f"[{self.name}] {msg}")

    def send_json(self, sock: socket.socket, addr: Tuple[str, int], payload: Dict[str, Any], label: str) -> None:
        """Encode and send a JSON datagram while logging the transmission."""
        self.trace(f"TX {label} -> {addr}: {payload}")
        sock.sendto(json.dumps(payload, separators=(",", ":")).encode("utf-8"), addr)

    def send_manager(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send one request to the manager and wait for its reply."""
        self.send_json(self.m_sock, self.manager_addr, payload, payload.get("cmd", "MGR"))
        try:
            data, _ = self.m_sock.recvfrom(65535)
        except socket.timeout:
            reply = {
                "cmd": str(payload.get("cmd", "MGR")),
                "status": FAIL,
                "reason": f"manager {self.manager_addr[0]}:{self.manager_addr[1]} did not respond before timeout",
            }
            self.trace(f"RX manager reply: {reply}")
            return reply
        reply = json.loads(data.decode("utf-8"))
        self.trace(f"RX manager reply: {reply}")
        return reply

    def send_peer(self, peer: PeerTuple, payload: Dict[str, Any], label: Optional[str] = None) -> None:
        """Send a JSON message to another peer in the system."""
        self.send_json(self.p_sock, (peer.ip, peer.pport), payload, label or str(payload.get("cmd", "P2P")))

    def my_tuple(self) -> PeerTuple:
        """Build this peer's current peer-to-peer identity tuple."""
        my_ip = get_my_ip_for_manager(self.manager_addr[0])
        return PeerTuple(self.name, my_ip, self.p_port)

    def update_ring(self, ring: List[PeerTuple], my_id: int) -> None:
        """Replace local ring metadata and recompute the right neighbor."""
        self.ring = list(ring)
        self.ring_size = len(ring)
        self.my_id = my_id
        if self.ring_size > 0:
            self.right_neighbor = self.ring[(self.my_id + 1) % self.ring_size]
        else:
            self.right_neighbor = None
        self.trace(
            f"ring updated: my_id={self.my_id} ring_size={self.ring_size} right_neighbor={self.right_neighbor.name if self.right_neighbor else None}"
        )

    def clear_local_hash(self) -> None:
        """Delete all locally stored DHT records and counters."""
        self.local_hashtable.clear()
        self.my_record_count = 0

    def set_dht_metadata(self, year: str, hash_size: int) -> None:
        """Record the dataset year and hash-table size for this DHT."""
        self.current_year = year
        self.hash_size = hash_size

    def register(self) -> Dict[str, Any]:
        """Register this peer with the manager."""
        my_ip = get_my_ip_for_manager(self.manager_addr[0])
        return self.send_manager(
            {
                "cmd": "REGISTER",
                "peer_name": self.name,
                "ip": my_ip,
                "mport": self.m_port,
                "pport": self.p_port,
            }
        )

    def deregister(self) -> Dict[str, Any]:
        """Ask the manager to remove this free peer from the system."""
        reply = self.send_manager({"cmd": "DEREGISTER", "peer_name": self.name})
        if reply.get("status") == SUCCESS:
            self._shutdown_allowed = True
        return reply

    def setup_dht(self, n: int, year: str) -> Dict[str, Any]:
        """Request DHT setup and, if leader, build the initial ring contents."""
        reply = self.send_manager({"cmd": "SETUP", "peer_name": self.name, "n": n, "year": year})
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

    def query_event(self, event_id: int) -> Optional[QueryResult]:
        """Run a hot-potato query for one storm event ID."""
        reply = self.send_manager({"cmd": "QUERY_DHT", "peer_name": self.name})
        if reply.get("status") != SUCCESS:
            self.trace(f"query-dht failed: {reply}")
            return None

        start_peer = dict_to_peer_tuple(reply["start_peer"])
        query_id = uuid.uuid4().hex
        event = threading.Event()
        with self._query_lock:
            self._pending_queries[query_id] = (event, None)

        payload = {
            "cmd": "FIND_EVENT",
            "query_id": query_id,
            "event_id": int(event_id),
            "requester": peer_tuple_to_dict(self.my_tuple()),
            "id_seq": [],
            "remaining_ids": [],
        }
        self.send_peer(start_peer, payload, "FIND_EVENT")

        if not event.wait(timeout=20.0):
            self.trace(f"query {query_id} timed out")
            with self._query_lock:
                self._pending_queries.pop(query_id, None)
            return None

        with self._query_lock:
            _evt, result = self._pending_queries.pop(query_id, (None, None))
        if result is None:
            return None
        self.print_query_result(result)
        return result

    def leave_dht(self) -> Dict[str, Any]:
        """Leave the DHT by tearing it down, renumbering survivors, and rebuilding."""
        reply = self.send_manager({"cmd": "LEAVE_DHT", "peer_name": self.name})
        if reply.get("status") != SUCCESS:
            return reply

        ring = [dict_to_peer_tuple(x) for x in reply["ring"]]
        year = str(reply["year"])
        self.current_year = year
        self.trace("leave-dht accepted by manager; starting teardown and rebuild")

        self._teardown_complete.clear()
        self._start_teardown(origin_name=self.name, purpose="leave")
        self._teardown_complete.wait(timeout=20.0)

        old_ring = list(ring)
        if self.name not in [p.name for p in old_ring]:
            self.trace("leave rebuild aborted: self not in old ring")
            return reply

        leave_index = next(i for i, p in enumerate(old_ring) if p.name == self.name)
        new_ring = [old_ring[(leave_index + offset) % len(old_ring)] for offset in range(1, len(old_ring))]
        if not new_ring:
            self.trace("leave rebuild aborted: resulting ring empty")
            return reply

        self._reset_complete.clear()
        reset_msg = {
            "cmd": "RESET_ID",
            "origin": self.name,
            "origin_tuple": peer_tuple_to_dict(self.my_tuple()),
            "ring": [peer_tuple_to_dict(p) for p in new_ring],
            "next_id": 0,
            "year": self.current_year,
        }
        self.send_peer(new_ring[0], reset_msg, "RESET_ID")
        self._reset_complete.wait(timeout=20.0)

        self.update_ring([], -1)
        self.clear_local_hash()

        self._rebuild_done.clear()
        rebuild_msg = {
            "cmd": "REBUILD_DHT",
            "origin": self.name,
            "ring": [peer_tuple_to_dict(p) for p in new_ring],
            "year": self.current_year,
        }
        self.send_peer(new_ring[0], rebuild_msg, "REBUILD_DHT")
        self._rebuild_done.wait(timeout=30.0)

        manager_reply = self.send_manager(
            {
                "cmd": "DHT_REBUILT",
                "peer_name": self.name,
                "new_leader": new_ring[0].name,
                "ring_names": [p.name for p in new_ring],
                "year": self.current_year,
            }
        )
        return manager_reply

    def join_dht(self) -> Dict[str, Any]:
        """Join an existing DHT using the project-defined rebuild protocol."""
        reply = self.send_manager({"cmd": "JOIN_DHT", "peer_name": self.name})
        if reply.get("status") != SUCCESS:
            return reply

        leader = dict_to_peer_tuple(reply["leader"])
        current_ring = [dict_to_peer_tuple(x) for x in reply["ring"]]
        self.current_year = str(reply["year"])
        self.trace(f"join-dht accepted by manager; leader={leader.name}")

        self._join_setid_done.clear()
        self._rebuild_done.clear()

        join_msg = {
            "cmd": "JOIN_REQUEST",
            "joiner": peer_tuple_to_dict(self.my_tuple()),
            "year": self.current_year,
            "ring": [peer_tuple_to_dict(p) for p in current_ring],
        }
        self.send_peer(leader, join_msg, "JOIN_REQUEST")

        self._rebuild_done.wait(timeout=30.0)
        if not self._rebuild_result:
            self.trace("join rebuild did not finish in time")
            return reply

        new_leader = str(self._rebuild_result["new_leader"])
        ring_names = list(self._rebuild_result["ring_names"])
        manager_reply = self.send_manager(
            {
                "cmd": "DHT_REBUILT",
                "peer_name": self.name,
                "new_leader": new_leader,
                "ring_names": ring_names,
                "year": self.current_year,
            }
        )
        return manager_reply

    def teardown_dht(self) -> Dict[str, Any]:
        """Delete the current DHT if this peer is the leader."""
        reply = self.send_manager({"cmd": "TEARDOWN_DHT", "peer_name": self.name})
        if reply.get("status") != SUCCESS:
            return reply

        self._teardown_complete.clear()
        self._start_teardown(origin_name=self.name, purpose="teardown")
        self._teardown_complete.wait(timeout=20.0)
        manager_reply = self.send_manager({"cmd": "TEARDOWN_COMPLETE", "peer_name": self.name})
        if manager_reply.get("status") == SUCCESS:
            self.update_ring([], -1)
            self.clear_local_hash()
            self.current_year = None
        return manager_reply

    def _broadcast_set_id(self, ring: List[PeerTuple]) -> None:
        """Assign IDs and full ring metadata to every non-leader peer."""
        with self._setid_ack_lock:
            self._setid_acks.clear()

        for i in range(1, len(ring)):
            target = ring[i]
            payload = {
                "cmd": "SET_ID",
                "id": i,
                "ring": [peer_tuple_to_dict(p) for p in ring],
                "year": self.current_year,
            }
            self.send_peer(target, payload, "SET_ID")

        deadline = time.time() + 10.0
        while time.time() < deadline:
            with self._setid_ack_lock:
                if len(self._setid_acks) >= len(ring) - 1:
                    break
            time.sleep(0.05)
        with self._setid_ack_lock:
            self.trace(f"SET_ID ACKs received: {len(self._setid_acks)}/{len(ring) - 1}")

    def _build_dht_as_leader(self, notify_manager: bool) -> None:
        """Load the dataset and distribute records across the ring as leader."""
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
            else:
                payload = {
                    "cmd": "STORE",
                    "pos": pos,
                    "target_id": target_id,
                    "record": rec,
                    "hash_size": hash_size,
                    "year": self.current_year,
                }
                assert self.right_neighbor is not None
                self.send_peer(self.right_neighbor, payload, "STORE")

        time.sleep(1.0)
        self._counts = {0: self.my_record_count}
        self._count_done.clear()
        if self.ring_size > 1 and self.right_neighbor is not None:
            self.send_peer(
                self.right_neighbor,
                {"cmd": "COUNT_TOKEN", "origin": self.name, "counts": {"0": self.my_record_count}},
                "COUNT_TOKEN",
            )
            self._count_done.wait(timeout=10.0)
        else:
            self._count_done.set()

        self.print_ring_counts()
        if notify_manager:
            reply = self.send_manager({"cmd": "DHT_COMPLETE", "peer_name": self.name})
            self.trace(f"DHT_COMPLETE -> {reply}")

    def print_ring_counts(self) -> None:
        """Print how many records each peer stores after a build or rebuild."""
        self.trace("=" * 60)
        self.trace("DHT setup/rebuild complete – record counts per node:")
        total = 0
        for i, peer in enumerate(self.ring):
            cnt = int(self._counts.get(i, 0))
            total += cnt
            self.trace(f"  node {i} ({peer.name}): {cnt} records")
        self.trace(f"Total records counted: {total}")
        self.trace("=" * 60)

    def _start_teardown(self, origin_name: str, purpose: str) -> None:
        """Inject a teardown token into the ring for teardown-like workflows."""
        if not self.ring or self.right_neighbor is None:
            self.clear_local_hash()
            self._teardown_complete.set()
            return
        self.trace(f"starting teardown purpose={purpose}")
        payload = {"cmd": "TEARDOWN", "origin": origin_name, "purpose": purpose}
        self.send_peer(self.right_neighbor, payload, "TEARDOWN")

    def _peer_receive_loop(self) -> None:
        """Receive peer-to-peer datagrams and dispatch them to handlers."""
        while not self.stop_event.is_set():
            try:
                data, addr = self.p_sock.recvfrom(65535)
            except Exception:
                continue
            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception:
                self.trace(f"RX invalid peer JSON from {addr}")
                continue
            self.trace(f"RX from {addr}: {payload}")
            self._handle_peer_message(payload, addr)

    def _handle_peer_message(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Dispatch a decoded peer message to its `_on_*` handler."""
        cmd = str(msg.get("cmd", "")).upper()
        handler = getattr(self, f"_on_{cmd.lower()}", None)
        if handler is None:
            self.trace(f"no handler for peer cmd {cmd}")
            return
        handler(msg, addr)

    def _on_set_id(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Install ring membership and this peer's assigned identifier."""
        ring = [dict_to_peer_tuple(x) for x in msg["ring"]]
        new_id = int(msg["id"])
        year = str(msg["year"])
        self.current_year = year
        self.update_ring(ring, new_id)
        ack = {"cmd": "SET_ID_ACK", "id": self.my_id, "peer_name": self.name}
        self.send_peer(ring[0], ack, "SET_ID_ACK")
        self._join_setid_done.set()

    def _on_set_id_ack(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Record that a peer acknowledged its assigned identifier."""
        ack_id = int(msg["id"])
        with self._setid_ack_lock:
            self._setid_acks.add(ack_id)

    def _on_store(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Store a forwarded record locally or relay it around the ring."""
        pos = int(msg["pos"])
        target_id = int(msg["target_id"])
        record = dict(msg["record"])
        self.hash_size = int(msg["hash_size"])
        self.current_year = str(msg["year"])

        if target_id == self.my_id:
            self.local_hashtable.setdefault(pos, []).append(record)
            self.my_record_count += 1
        else:
            if self.right_neighbor is not None:
                self.send_peer(self.right_neighbor, msg, "STORE")

    def _on_count_token(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Accumulate per-node record counts as a token circles the ring."""
        counts_raw = msg.get("counts", {})
        counts = {int(k): int(v) for k, v in counts_raw.items()}
        origin = str(msg["origin"])

        if self.name == origin:
            self._counts = counts
            self._count_done.set()
            return

        counts[self.my_id] = self.my_record_count
        new_msg = {"cmd": "COUNT_TOKEN", "origin": origin, "counts": {str(k): v for k, v in counts.items()}}
        if self.right_neighbor is not None:
            self.send_peer(self.right_neighbor, new_msg, "COUNT_TOKEN")

    def _find_local_record(self, pos: int, event_id: int) -> Optional[Dict[str, str]]:
        """Search one local hash bucket for an exact event ID match."""
        bucket = self.local_hashtable.get(pos, [])
        for rec in bucket:
            try:
                if int(rec["event_id"]) == event_id:
                    return rec
            except Exception:
                continue
        return None

    def _on_find_event(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Process one hot-potato query step and forward or answer it."""
        query_id = str(msg["query_id"])
        event_id = int(msg["event_id"])
        requester = dict_to_peer_tuple(msg["requester"])
        id_seq = [int(x) for x in msg.get("id_seq", [])]
        remaining_ids = [int(x) for x in msg.get("remaining_ids", [])]

        if self.hash_size <= 0 or self.ring_size <= 0:
            result = QueryResult(False, query_id, event_id, id_seq, None, "DHT metadata unavailable")
            self.send_peer(requester, {"cmd": "FIND_RESULT", **asdict(result)}, "FIND_RESULT")
            return

        if not id_seq:
            id_seq = [self.my_id]
            remaining_ids = [i for i in range(self.ring_size) if i != self.my_id]
        elif not id_seq or id_seq[-1] != self.my_id:
            id_seq.append(self.my_id)
            remaining_ids = [i for i in remaining_ids if i != self.my_id]

        pos = event_id % self.hash_size
        record = self._find_local_record(pos, event_id)
        if record is not None:
            result = QueryResult(True, query_id, event_id, id_seq, record, "")
            self.send_peer(requester, {"cmd": "FIND_RESULT", **asdict(result)}, "FIND_RESULT")
            return

        if not remaining_ids:
            result = QueryResult(False, query_id, event_id, id_seq, None, "not found")
            self.send_peer(requester, {"cmd": "FIND_RESULT", **asdict(result)}, "FIND_RESULT")
            return

        next_id = random.choice(remaining_ids)
        remaining_ids = [i for i in remaining_ids if i != next_id]
        next_peer = self.ring[next_id]
        forward = {
            "cmd": "FIND_EVENT",
            "query_id": query_id,
            "event_id": event_id,
            "requester": peer_tuple_to_dict(requester),
            "id_seq": id_seq,
            "remaining_ids": remaining_ids,
        }
        self.send_peer(next_peer, forward, "FIND_EVENT")

    def _on_find_result(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Complete a locally pending query with the returned search result."""
        result = QueryResult(
            ok=bool(msg["ok"]),
            query_id=str(msg["query_id"]),
            event_id=int(msg["event_id"]),
            id_seq=[int(x) for x in msg.get("id_seq", [])],
            record=msg.get("record"),
            error=str(msg.get("error", "")),
        )
        with self._query_lock:
            pair = self._pending_queries.get(result.query_id)
            if pair is None:
                return
            event, _ = pair
            self._pending_queries[result.query_id] = (event, result)
            event.set()

    def _on_teardown(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Handle the teardown token by clearing local state and forwarding it."""
        origin = str(msg["origin"])
        purpose = str(msg.get("purpose", "teardown"))

        if self.name == origin:
            self.clear_local_hash()
            self._teardown_complete.set()
            self.trace(f"teardown token returned to origin; purpose={purpose}")
            return

        self.clear_local_hash()
        if self.right_neighbor is not None:
            self.send_peer(self.right_neighbor, msg, "TEARDOWN")

    def _on_reset_id(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Renumber surviving peers during the leave-dht rebuild flow."""
        origin = str(msg["origin"])
        origin_tuple = dict_to_peer_tuple(msg["origin_tuple"])
        ring = [dict_to_peer_tuple(x) for x in msg["ring"]]
        next_id = int(msg["next_id"])
        year = str(msg["year"])
        self.current_year = year

        my_new_id = next_id
        self.update_ring(ring, my_new_id)

        if next_id + 1 >= len(ring):
            self.send_peer(origin_tuple, {"cmd": "RESET_DONE", "origin": origin}, "RESET_DONE")
            return

        next_msg = {
            "cmd": "RESET_ID",
            "origin": origin,
            "origin_tuple": peer_tuple_to_dict(origin_tuple),
            "ring": [peer_tuple_to_dict(p) for p in ring],
            "next_id": next_id + 1,
            "year": year,
        }
        if self.right_neighbor is not None:
            self.send_peer(self.right_neighbor, next_msg, "RESET_ID")

    def _on_reset_done(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Mark the reset-id phase as finished for the leaving peer."""
        self.trace("received RESET_DONE")
        self._reset_complete.set()

    def _on_rebuild_dht(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Rebuild the DHT after a leave operation reaches the new leader."""
        origin = str(msg["origin"])
        ring = [dict_to_peer_tuple(x) for x in msg["ring"]]
        year = str(msg["year"])
        self.current_year = year

        if not ring or ring[0].name != self.name:
            self.trace("REBUILD_DHT ignored because this peer is not the new leader")
            return

        self.update_ring(ring, 0)
        self._broadcast_set_id(ring)
        self._build_dht_as_leader(notify_manager=False)

        origin_peer = next((p for p in ring if p.name == origin), None)
        if origin_peer is None:
            origin_peer = dict_to_peer_tuple(msg.get("origin_tuple", {"name": origin, "ip": addr[0], "pport": addr[1]}))
        done_msg = {
            "cmd": "REBUILD_DONE",
            "origin": origin,
            "new_leader": self.name,
            "ring_names": [p.name for p in ring],
            "year": year,
        }
        self.send_peer(origin_peer, done_msg, "REBUILD_DONE")

    def _on_rebuild_done(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Store rebuild completion data and wake the waiting command path."""
        self._rebuild_result = {
            "new_leader": str(msg["new_leader"]),
            "ring_names": list(msg["ring_names"]),
            "year": str(msg.get("year", self.current_year or "")),
        }
        self.current_year = self._rebuild_result["year"]
        self._rebuild_done.set()

    def _on_join_request(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Handle a join request by expanding the ring and rebuilding the DHT."""
        if self.my_id != 0:
            self.trace("JOIN_REQUEST ignored because this peer is not leader")
            return

        joiner = dict_to_peer_tuple(msg["joiner"])
        year = str(msg["year"])
        self.current_year = year
        old_ring = list(self.ring)
        if not old_ring:
            self.trace("JOIN_REQUEST ignored because no DHT exists locally")
            return

        self.trace(f"processing JOIN_REQUEST from {joiner.name}")
        self._teardown_complete.clear()
        self._start_teardown(origin_name=self.name, purpose="join")
        self._teardown_complete.wait(timeout=20.0)

        new_ring = old_ring + [joiner]

        self.update_ring(new_ring, 0)
        set_joiner = {
            "cmd": "SET_ID",
            "id": len(new_ring) - 1,
            "ring": [peer_tuple_to_dict(p) for p in new_ring],
            "year": self.current_year,
        }
        self.send_peer(joiner, set_joiner, "SET_ID")
        time.sleep(0.3)

        self._broadcast_set_id(new_ring)
        self._build_dht_as_leader(notify_manager=False)

        done = {
            "cmd": "REBUILD_DONE",
            "origin": joiner.name,
            "new_leader": self.name,
            "ring_names": [p.name for p in new_ring],
            "year": self.current_year,
        }
        self.send_peer(joiner, done, "REBUILD_DONE")

    def print_query_result(self, result: QueryResult) -> None:
        """Print a query result in the format expected by the project."""
        if not result.ok or not result.record:
            self.trace(f"Storm event {result.event_id} not found in the DHT.")
            if result.id_seq:
                self.trace(f"id-seq: {' -> '.join(map(str, result.id_seq))}")
            return

        self.trace(f"query SUCCESS for event_id={result.event_id}")
        for field in FIELD_ORDER:
            self.trace(f"  {FIELD_LABELS[field]}: {result.record.get(field, '')}")
        self.trace(f"  id-seq: {' -> '.join(map(str, result.id_seq))}")

    def repl(self) -> None:
        """Run the interactive stdin command loop for a peer process."""
        self.trace("commands:")
        self.trace("  setup <n> <year>")
        self.trace("  query <event_id>")
        self.trace("  leave")
        self.trace("  join")
        self.trace("  teardown")
        self.trace("  deregister")
        self.trace("  quit")

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
                    self.setup_dht(int(parts[1]), parts[2])
                elif cmd == "query" and len(parts) == 2:
                    self.query_event(int(parts[1]))
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
                    self.trace("quit requested")
                    break
                else:
                    self.trace("unknown command")
            except Exception as exc:
                self.trace(f"command failed: {exc}")

        self.stop_event.set()
        try:
            self.p_sock.close()
        except Exception:
            pass
        try:
            self.m_sock.close()
        except Exception:
            pass


"""
    --- Main ---
"""


def main() -> None:
    """Parse CLI mode and start either the manager or a peer."""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python dht_full_submission.py manager <manager_port>")
        print("  python dht_full_submission.py peer <manager_host> <manager_port> <m_port> <p_port> <peer_name>")
        sys.exit(1)

    mode = sys.argv[1].lower()
    if mode == "manager":
        if len(sys.argv) != 3:
            print("Usage: python dht_full_submission.py manager <manager_port>")
            sys.exit(1)
        DHTManager(int(sys.argv[2])).run()
        return

    if mode == "peer":
        if len(sys.argv) != 7:
            print("Usage: python dht_full_submission.py peer <manager_host> <manager_port> <m_port> <p_port> <peer_name>")
            sys.exit(1)
        peer = DHTPeer(
            manager_host=sys.argv[2],
            manager_port=int(sys.argv[3]),
            m_port=int(sys.argv[4]),
            p_port=int(sys.argv[5]),
            my_name=sys.argv[6],
        )
        reply = peer.register()
        if reply.get("status") != SUCCESS:
            print(f"[{peer.name}] register failed: {reply}")
            sys.exit(1)
        peer.repl()
        return

    print(f"Unknown mode: {mode}")
    sys.exit(1)


if __name__ == "__main__":
    main()
