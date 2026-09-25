"""Consistent Hashing Ring with Virtual Nodes.
Provides deterministic, uniform object key and chunk placement across distributed storage nodes with minimal remapping.

Architecture:
- Ring space: 32-bit integer keyspace [0, 2^32 - 1]
- Virtual nodes: Configurable replicas per physical node (default: 100 vnodes) to prevent hot spots
- Lookup: O(log V) binary search on sorted ring
- Preference List: Yields the next N distinct healthy physical nodes clockwise for replica/chunk distribution
"""
import bisect
import hashlib
from typing import Dict, List, Set, Optional, Tuple, Any

def hash_key(key: str) -> int:
    """Computes a 32-bit integer hash for a string key using MD5."""
    digest = hashlib.md5(key.encode('utf-8')).hexdigest()
    return int(digest[:8], 16)

class ConsistentHashRing:
    def __init__(self, vnodes: int = 100):
        self.vnodes = vnodes
        self.ring: List[int] = []              # Sorted list of hash points on the ring
        self.vnode_map: Dict[int, str] = {}    # Maps hash point -> physical node_id
        self.nodes: Set[str] = set()           # Set of registered physical nodes
        self.node_states: Dict[str, str] = {}  # node_id -> 'ALIVE' | 'DEGRADED' | 'DEAD'

    def add_node(self, node_id: str, state: str = "ALIVE", weight: int = 1):
        """Add a physical storage node to the ring with virtual nodes."""
        self.nodes.add(node_id)
        self.node_states[node_id] = state
        
        num_vnodes = self.vnodes * max(1, weight)
        for i in range(num_vnodes):
            vkey = f"{node_id}-vnode-{i}"
            h = hash_key(vkey)
            self.ring.append(h)
            self.vnode_map[h] = node_id

        self.ring.sort()

    def remove_node(self, node_id: str):
        """Remove a physical storage node and all its virtual nodes from the ring."""
        if node_id not in self.nodes:
            return

        self.nodes.remove(node_id)
        self.node_states.pop(node_id, None)

        new_ring = []
        new_map = {}
        for h in self.ring:
            if self.vnode_map[h] != node_id:
                new_ring.append(h)
                new_map[h] = self.vnode_map[h]

        self.ring = new_ring
        self.vnode_map = new_map

    def update_node_state(self, node_id: str, state: str):
        """Update node liveness state (ALIVE, DEGRADED, DEAD)."""
        if node_id in self.nodes:
            self.node_states[node_id] = state

    def get_primary_node(self, key: str) -> Optional[str]:
        """Find the primary physical node responsible for a given key."""
        nodes = self.get_preference_list(key, count=1)
        return nodes[0] if nodes else None

    def get_preference_list(self, key: str, count: int = 3, only_alive: bool = True) -> List[str]:
        """Traverse the hash ring clockwise from hash(key) and return the next N distinct physical nodes.
        Used for replica placement and erasure-coded chunk dispersal.
        """
        if not self.ring:
            return []

        h = hash_key(key)
        # Find index of first vnode >= h
        idx = bisect.bisect_right(self.ring, h) % len(self.ring)

        selected_nodes: List[str] = []
        seen: Set[str] = set()
        scanned = 0
        total_vnodes = len(self.ring)

        while len(selected_nodes) < count and scanned < total_vnodes:
            curr_hash = self.ring[idx]
            physical_node = self.vnode_map[curr_hash]

            if physical_node not in seen:
                seen.add(physical_node)
                if not only_alive or self.node_states.get(physical_node) in ("ALIVE", "DEGRADED"):
                    selected_nodes.append(physical_node)

            idx = (idx + 1) % len(self.ring)
            scanned += 1

        return selected_nodes

    def get_ring_topology(self) -> Dict[str, Any]:
        """Returns visual ring topology data for dashboard visualization."""
        node_counts = {nid: 0 for nid in self.nodes}
        for nid in self.vnode_map.values():
            if nid in node_counts:
                node_counts[nid] += 1

        return {
            "total_vnodes": len(self.ring),
            "nodes": [
                {
                    "node_id": nid,
                    "state": self.node_states.get(nid, "ALIVE"),
                    "vnode_count": node_counts.get(nid, 0),
                    "ring_coverage_pct": round((node_counts.get(nid, 0) / max(1, len(self.ring))) * 100, 2)
                }
                for nid in sorted(self.nodes)
            ]
        }

consistent_hash_ring = ConsistentHashRing(vnodes=100)
