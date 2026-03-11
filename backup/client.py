"""
backup/client.py — BackupClient: node discovery and part replication.
"""

from __future__ import annotations

import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests


class BackupClient:
    def __init__(self, nodes: list[str] | None = None):
        self.nodes: list[str] = nodes or []
        self._lock = threading.Lock()

    # ── Discovery ─────────────────────────────────────────────────────────────

    def discover(
        self,
        on_found: callable | None = None,
        timeout: float = 0.4,
    ) -> list[str]:
        """
        Parallel LAN scan for backup nodes running the Flask server.
        Calls on_found(ip_list) when done (safe to use for UI update).
        Returns list of discovered IPs.
        """
        found: list[str] = []
        try:
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            subnet = ".".join(local_ip.split(".")[:-1])
            ips = [f"{subnet}.{i}" for i in range(1, 255)]

            def check(ip: str) -> Optional[str]:
                try:
                    r = requests.get(f"http://{ip}:5000/health", timeout=timeout)
                    return ip if r.status_code == 200 else None
                except Exception:
                    return None

            with ThreadPoolExecutor(max_workers=64) as pool:
                for result in as_completed([pool.submit(check, ip) for ip in ips]):
                    res = result.result()
                    if res:
                        found.append(res)
        except Exception:
            pass

        with self._lock:
            self.nodes = found

        if on_found:
            try:
                on_found(found)
            except Exception:
                pass

        return found

    def online_nodes(self) -> list[str]:
        """Quick health check on known nodes."""
        alive: list[str] = []
        with self._lock:
            nodes = list(self.nodes)
        for node in nodes:
            try:
                r = requests.get(f"http://{node}:5000/health", timeout=1.0)
                if r.status_code == 200:
                    alive.append(node)
            except Exception:
                pass
        return alive

    # ── Upload ────────────────────────────────────────────────────────────────

    def send_part(self, node: str, path: str) -> bool:
        try:
            with open(path, "rb") as f:
                r = requests.post(
                    f"http://{node}:5000/store",
                    files={"file": (os.path.basename(path), f)},
                    timeout=30,
                )
            return r.ok
        except Exception:
            return False

    def send_index(self, node: str, index_path: str) -> bool:
        try:
            with open(index_path, "rb") as f:
                r = requests.post(
                    f"http://{node}:5000/store_index",
                    files={"file": f},
                    timeout=10,
                )
            return r.ok
        except Exception:
            return False

    def replicate(self, nodes: list[str], parts: list[str], index_path: str) -> None:
        """Send all parts + index to every online node in parallel."""
        tasks: list[tuple[str, str]] = [
            (node, part) for node in nodes for part in parts
        ]
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda t: self.send_part(*t), tasks))
        for node in nodes:
            self.send_index(node, index_path)

    # ── Server file list ──────────────────────────────────────────────────────

    def list_files(self, node: str) -> list[str]:
        try:
            r = requests.get(f"http://{node}:5000/list", timeout=5)
            return r.json()
        except Exception:
            return []

    def list_parts(self, node: str) -> list[str]:
        try:
            r = requests.get(f"http://{node}:5000/list_parts", timeout=5)
            return r.json()
        except Exception:
            return []

    def get_index(self, node: str) -> dict:
        try:
            r = requests.get(f"http://{node}:5000/get_index", timeout=5)
            return r.json()
        except Exception:
            return {}

    def download_part(self, node: str, filename: str, dest_dir: str) -> Optional[str]:
        try:
            r = requests.get(f"http://{node}:5000/get_part/{filename}", timeout=60, stream=True)
            dest = os.path.join(dest_dir, filename)
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1_048_576):
                    if chunk:
                        f.write(chunk)
            return dest
        except Exception:
            return None

    def delete_file(self, node: str, filename: str) -> bool:
        try:
            r = requests.delete(f"http://{node}:5000/delete/{filename}", timeout=5)
            return r.ok
        except Exception:
            return False


    def backup_all(self, parts: list, index_path: str) -> None:
        """Send all parts + index to every known online node."""
        online = self.online_nodes()
        if not online:
            return
        for node in online:
            for p in parts:
                self.send_part(node, p)
            self.send_index(node, index_path)
