import csv
import math
import os
import random
import socket
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

MSG_SEPARATOR = "|"
SUCCESS = "OK"
FAIL = "FAIL"

PREFIX_REGISTER = "REGISTER"
PREFIX_SETUP = "SETUP"
PREFIX_DHT_COMPLETE = "DHT-COMPLETE"

PREFIX_SET_ID = "SET-ID"
PREFIX_SET_ID_ACK = "SET-ID-ACK"

PREFIX_STORE = "STORE"
PREFIX_COUNT_TOKEN  = "COUNT-TOKEN"

# helpers -----

def next_prime(x: int) -> int:
    if x <= 2:
        return 2

    def is_prime(n: int) -> bool:
        if n % 2 == 0:
            return n == 2
        r = int(math.isqrt(n))
        for i in range(3, r + 1, 2):
            if n % i == 0:
                return False
        return True

    n = x
    if n % 2 == 0:
        n += 1
    while not is_prime(n):
        n += 2
    return n


def format_peer_tuple(name: str, ip: str, pport: int) -> str:
    return f"{name},{ip},{pport}"


def parse_peer_tuple(s: str) -> Tuple[str, str, int]:
    name, ip, port = s.split(",")
    return name, ip, int(port)


def get_my_ip_for_manager(manager_ip: str) -> str:
    tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        tmp.connect((manager_ip, 9))
        return tmp.getsockname()[0]
    finally:
        tmp.close()


def load_records_for_year(year: str) -> List[Dict[str, str]]:
    preferred = f"details-{year}.csv"
    candidates = []
    if os.path.exists(preferred):
        candidates = [preferred]
    else:
        for fn in os.listdir("."):
            if fn.lower().endswith(".csv") and year in fn:
                candidates.append(fn)

    if not candidates:
        raise FileNotFoundError(f"Could not find details-{year}.csv (or any CSV containing {year}) in current directory.")

    filename = candidates[0]

    records: List[Dict[str, str]] = []
    with open(filename, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if len(row) >= 14:
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
                except:
                    continue
                records.append(rec)
            else:
                continue

    return records

# manager ----------

@dataclass
class PeerInfo:
    name: str
    ip: str
    mport: int
    pport: int
    state: str  #(FREE, LEADER, INDHT)


class DHTManager:
    def __init__(self, port: int):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        print(f"[Manager] listening on UDP port {port}")

        self.peers: Dict[str, PeerInfo] = {}
        self.used_ports: set[int] = set()

        self.current_ring: Optional[List[PeerInfo]] = None
        self.building: bool = False
        self.leader_name: Optional[str] = None

    def send_to(self, addr: Tuple[str, int], msg: str):
        self.sock.sendto(msg.encode("utf-8"), addr)

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(8192)
            line = data.decode("utf-8").strip()
            if not line:
                continue

            parts = line.split(MSG_SEPARATOR)
            cmd = parts[0]

            if self.building and cmd != PREFIX_DHT_COMPLETE:
                self.send_to(addr, f"{cmd}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}building")
                continue

            if cmd == PREFIX_REGISTER:
                if len(parts) != 5:
                    self.send_to(addr, f"{PREFIX_REGISTER}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}bad format")
                    continue

                _, name, ip, mport_str, pport_str = parts
                try:
                    mport = int(mport_str)
                    pport = int(pport_str)
                except:
                    self.send_to(addr, f"{PREFIX_REGISTER}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}bad ports")
                    continue

                if name in self.peers:
                    self.send_to(addr, f"{PREFIX_REGISTER}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}name exists")
                    continue

                if mport <= 0 or pport <= 0:
                    self.send_to(addr, f"{PREFIX_REGISTER}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}ports must be >0")
                    continue

                if mport in self.used_ports or pport in self.used_ports:
                    self.send_to(addr, f"{PREFIX_REGISTER}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}port in use")
                    continue

                self.used_ports.add(mport)
                self.used_ports.add(pport)

                self.peers[name] = PeerInfo(name=name, ip=ip, mport=mport, pport=pport, state="FREE")
                self.send_to(addr, f"{PREFIX_REGISTER}{MSG_SEPARATOR}{SUCCESS}")
                print(f"[Manager] Registered {name} ip={ip} m={mport} p={pport}")

            elif cmd == PREFIX_SETUP:
                # SETUP|leader_name|n|year
                if self.current_ring is not None:
                    self.send_to(addr, f"{PREFIX_SETUP}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}DHT exists")
                    continue
                if len(parts) != 4:
                    self.send_to(addr, f"{PREFIX_SETUP}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}bad format")
                    continue

                _, leader_name, n_str, year = parts
                try:
                    n = int(n_str)
                except:
                    self.send_to(addr, f"{PREFIX_SETUP}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}bad n")
                    continue

                if leader_name not in self.peers or n < 3:
                    self.send_to(addr, f"{PREFIX_SETUP}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}bad leader or n")
                    continue

                leader = self.peers[leader_name]
                if leader.state != "FREE":
                    self.send_to(addr, f"{PREFIX_SETUP}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}leader not free")
                    continue

                free_peers = [p for p in self.peers.values() if p.state == "FREE" and p.name != leader_name]
                if len(free_peers) < n - 1:
                    self.send_to(addr, f"{PREFIX_SETUP}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}not enough FREE peers")
                    continue

                chosen = random.sample(free_peers, n - 1)
                ring = [leader] + chosen

                leader.state = "LEADER"
                for p in chosen:
                    p.state = "INDHT"

                self.current_ring = ring
                self.building = True
                self.leader_name = leader_name

                tuples_str = MSG_SEPARATOR.join(format_peer_tuple(p.name, p.ip, p.pport) for p in ring)
                reply = f"{PREFIX_SETUP}{MSG_SEPARATOR}{SUCCESS}{MSG_SEPARATOR}{tuples_str}"
                self.send_to(addr, reply)  # reply to sender (leader m-port)
                print(f"[Manager] setup-dht accepted leader={leader_name} n={n} year={year}")

            elif cmd == PREFIX_DHT_COMPLETE:
                if len(parts) != 2:
                    self.send_to(addr, f"{PREFIX_DHT_COMPLETE}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}bad format")
                    continue
                _, name = parts
                if not self.current_ring or not self.leader_name or name != self.leader_name:
                    self.send_to(addr, f"{PREFIX_DHT_COMPLETE}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}not leader")
                    continue

                self.building = False
                self.send_to(addr, f"{PREFIX_DHT_COMPLETE}{MSG_SEPARATOR}{SUCCESS}")
                print(f"[Manager] DHT complete (leader={name})")

            else:
                self.send_to(addr, f"{cmd}{MSG_SEPARATOR}{FAIL}{MSG_SEPARATOR}unknown command")

# Peer --------

class DHTPeer:
    def __init__(self, manager_host: str, manager_port: int, m_port: int, p_port: int, my_name: str):
        self.name = my_name
        self.manager_addr = (manager_host, manager_port)
        self.m_port = m_port
        self.p_port = p_port

        self.m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.m_sock.bind(("0.0.0.0", m_port))
        self.m_sock.settimeout(5.0)

        self.p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.p_sock.bind(("0.0.0.0", p_port))

        print(f"[{my_name}] m-port={m_port} p-port={p_port}")

        self.my_id: int = -1
        self.ring_size: int = 0
        self.ring: List[Dict[str, object]] = []  # dicts with name/ip/pport
        self.next_addr: Optional[Tuple[str, int]] = None

        self.local_hashtable: Dict[int, List[Dict[str, str]]] = {}
        self.my_record_count: int = 0

        self._ack_lock = threading.Lock()
        self._acks_received: set[int] = set()

        self._count_done = threading.Event()
        self._counts: Dict[int, int] = {}

        threading.Thread(target=self._peer_receive_loop, daemon=True).start()

    def _send_manager(self, msg: str) -> str:
        self.m_sock.sendto(msg.encode("utf-8"), self.manager_addr)
        data, _ = self.m_sock.recvfrom(8192)
        return data.decode("utf-8").strip()

    def register(self):
        my_ip = get_my_ip_for_manager(self.manager_addr[0])
        msg = f"{PREFIX_REGISTER}{MSG_SEPARATOR}{self.name}{MSG_SEPARATOR}{my_ip}{MSG_SEPARATOR}{self.m_port}{MSG_SEPARATOR}{self.p_port}"
        reply = self._send_manager(msg)
        print(f"[{self.name}] register → {reply}")
        return reply

    def setup_dht(self, n: int, year: str):
        msg = f"{PREFIX_SETUP}{MSG_SEPARATOR}{self.name}{MSG_SEPARATOR}{n}{MSG_SEPARATOR}{year}"
        reply = self._send_manager(msg)
        parts = reply.split(MSG_SEPARATOR)

        if len(parts) < 2 or parts[1] != SUCCESS:
            print(f"[{self.name}] setup failed: {reply}")
            return

        tuples = parts[2:]
        self.ring = [dict(zip(["name", "ip", "pport"], parse_peer_tuple(t))) for t in tuples]
        self.ring_size = len(self.ring)

        if self.ring[0]["name"] != self.name:
            print(f"[{self.name}] Not leader; waiting for SET-ID...")
            return

        self.my_id = 0
        self.next_addr = (self.ring[1]["ip"], int(self.ring[1]["pport"]))

        ring_str = MSG_SEPARATOR.join(format_peer_tuple(d["name"], d["ip"], int(d["pport"])) for d in self.ring)

        with self._ack_lock:
            self._acks_received.clear()

        for i in range(1, self.ring_size):
            target = self.ring[i]
            setid = f"{PREFIX_SET_ID}{MSG_SEPARATOR}{i}{MSG_SEPARATOR}{self.ring_size}{MSG_SEPARATOR}{ring_str}"
            for _ in range(3):
                self.p_sock.sendto(setid.encode("utf-8"), (target["ip"], int(target["pport"])))
                time.sleep(0.05)

        deadline = time.time() + 3.0
        while time.time() < deadline:
            with self._ack_lock:
                if len(self._acks_received) >= self.ring_size - 1:
                    break
            time.sleep(0.05)

        with self._ack_lock:
            print(f"[{self.name}] SET-ID ACKs received: {len(self._acks_received)}/{self.ring_size-1}")

        records = load_records_for_year(year)
        l = len(records)
        s = next_prime(2 * l + 1)
        print(f"[{self.name}] Loaded {l} records for {year}; hash table size s={s}")

        self.local_hashtable.clear()
        self.my_record_count = 0

        for rec in records:
            eid = int(rec["event_id"])
            pos = eid % s
            target_id = pos % self.ring_size

            store_msg = MSG_SEPARATOR.join([
                PREFIX_STORE, str(pos),
                rec["event_id"], rec["state"], rec["year"], rec["month"],
                rec["event_type"], rec["cz_type"], rec["cz_name"],
                rec["inj_direct"], rec["inj_indirect"],
                rec["death_direct"], rec["death_indirect"],
                rec["damage_property"], rec["damage_crops"], rec["tor_f_scale"]
            ])

            if target_id == 0:
                self.local_hashtable.setdefault(pos, []).append(rec)
                self.my_record_count += 1
            else:
                self.p_sock.sendto(store_msg.encode("utf-8"), self.next_addr)

        time.sleep(1.5)

        self._counts = {0: self.my_record_count}
        self._count_done.clear()
        token = f"{PREFIX_COUNT_TOKEN}{MSG_SEPARATOR}0:{self.my_record_count}"
        self.p_sock.sendto(token.encode("utf-8"), self.next_addr)

        if not self._count_done.wait(timeout=5.0):
            print(f"[{self.name}] WARNING: count token did not return in time; printing partial counts")

        print("\n" + "=" * 60)
        print("DHT setup complete – record counts per node:")
        total = 0
        for i in range(self.ring_size):
            nm = self.ring[i]["name"]
            cnt = self._counts.get(i, 0)
            print(f"  Node {i} ({nm}): {cnt} records")
            total += cnt
        print(f"Total records counted: {total}")
        print("=" * 60 + "\n")

        complete = f"{PREFIX_DHT_COMPLETE}{MSG_SEPARATOR}{self.name}"
        rep2 = self._send_manager(complete)
        print(f"[{self.name}] dht-complete → {rep2}")

    def _peer_receive_loop(self):
        while True:
            data, addr = self.p_sock.recvfrom(65535)
            line = data.decode("utf-8", errors="ignore").strip()
            if not line:
                continue
            self._handle_peer_message(line, addr)

    def _handle_peer_message(self, line: str, addr: Tuple[str, int]):
        parts = line.split(MSG_SEPARATOR)
        cmd = parts[0]

        if cmd == PREFIX_SET_ID:
            if len(parts) < 4:
                return
            _, id_str, size_str, *tuples = parts
            self.my_id = int(id_str)
            self.ring_size = int(size_str)
            self.ring = [dict(zip(["name", "ip", "pport"], parse_peer_tuple(t))) for t in tuples]
            next_idx = (self.my_id + 1) % self.ring_size
            nxt = self.ring[next_idx]
            self.next_addr = (nxt["ip"], int(nxt["pport"]))

            leader = self.ring[0]
            ack = f"{PREFIX_SET_ID_ACK}{MSG_SEPARATOR}{self.my_id}{MSG_SEPARATOR}{self.name}"
            self.p_sock.sendto(ack.encode("utf-8"), (leader["ip"], int(leader["pport"])))

            print(f"[{self.name}] SET-ID received: id={self.my_id} ring={self.ring_size} next={nxt['name']}")

        elif cmd == PREFIX_SET_ID_ACK:
            if self.my_id != 0:
                return
            if len(parts) != 3:
                return
            _, id_str, _nm = parts
            with self._ack_lock:
                self._acks_received.add(int(id_str))

        elif cmd == PREFIX_STORE:
            if len(parts) < 16:
                return
            if self.my_id < 0 or self.next_addr is None:
                return

            pos = int(parts[1])
            rec = {
                "event_id": parts[2],
                "state": parts[3],
                "year": parts[4],
                "month": parts[5],
                "event_type": parts[6],
                "cz_type": parts[7],
                "cz_name": parts[8],
                "inj_direct": parts[9],
                "inj_indirect": parts[10],
                "death_direct": parts[11],
                "death_indirect": parts[12],
                "damage_property": parts[13],
                "damage_crops": parts[14],
                "tor_f_scale": parts[15],
            }

            target_id = pos % self.ring_size
            if target_id == self.my_id:
                self.local_hashtable.setdefault(pos, []).append(rec)
                self.my_record_count += 1
            else:
                self.p_sock.sendto(line.encode("utf-8"), self.next_addr)

        elif cmd == PREFIX_COUNT_TOKEN:
            if self.my_id < 0 or self.next_addr is None or len(parts) != 2:
                return

            counts_str = parts[1]
            counts: Dict[int, int] = {}
            if counts_str.strip():
                for item in counts_str.split(","):
                    i_s, c_s = item.split(":")
                    counts[int(i_s)] = int(c_s)

            if self.my_id == 0:
                self._counts = counts
                self._count_done.set()
                return

            if self.my_id not in counts:
                counts[self.my_id] = self.my_record_count

            new_counts_str = ",".join(f"{i}:{counts[i]}" for i in sorted(counts.keys()))
            new_token = f"{PREFIX_COUNT_TOKEN}{MSG_SEPARATOR}{new_counts_str}"
            self.p_sock.sendto(new_token.encode("utf-8"), self.next_addr)

# main ---------

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("python dht_milestone.py manager <manager_port>")
        print("python dht_milestone.py peer <manager_host> <manager_port> <m_port> <p_port> <peer_name>")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "manager":
        if len(sys.argv) != 3:
            print("Usage: python dht_milestone.py manager <manager_port>")
            sys.exit(1)
        port = int(sys.argv[2])
        DHTManager(port).run()

    elif mode == "peer":
        if len(sys.argv) != 7:
            print("Usage: python dht_milestone.py peer <manager_host> <manager_port> <m_port> <p_port> <peer_name>")
            sys.exit(1)

        mgr_host = sys.argv[2]
        mgr_port = int(sys.argv[3])
        m_port = int(sys.argv[4])
        p_port = int(sys.argv[5])
        name = sys.argv[6]

        peer = DHTPeer(mgr_host, mgr_port, m_port, p_port, name)

        peer.register()

        print(f"\n[{name}] commands:")
        print("setup <n> <year> (test = setup 3 1950)\n")
        print("quit\n")

        while True:
            try:
                line = input(f"[{name}]> ").strip()
                if not line:
                    continue
                words = line.split()
                cmd = words[0].lower()
                if cmd == "setup" and len(words) == 3:
                    peer.setup_dht(int(words[1]), words[2])
                elif cmd in ("quit", "q", "exit"):
                    break
            except KeyboardInterrupt:
                break

    else:
        print("Unknown mode:", mode)
        sys.exit(1)

if __name__ == "__main__":
    main()