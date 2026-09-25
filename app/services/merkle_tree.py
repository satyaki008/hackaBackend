"""Binary Merkle Tree for Chunked Cryptographic Integrity & Anti-Entropy Synchronization."""
import hashlib
from typing import List, Optional, Tuple, Dict, Any

def sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def combine_hashes(left: str, right: str) -> str:
    combined = (left + right).encode('utf-8')
    return hashlib.sha256(combined).hexdigest()

class MerkleNode:
    def __init__(self, hash_val: str, left: Optional['MerkleNode'] = None, right: Optional['MerkleNode'] = None):
        self.hash = hash_val
        self.left = left
        self.right = right

class MerkleTree:
    def __init__(self, leaf_hashes: List[str]):
        self.leaf_hashes = leaf_hashes
        self.root = self._build_tree(leaf_hashes)

    def _build_tree(self, hashes: List[str]) -> Optional[MerkleNode]:
        if not hashes:
            return None
        
        nodes = [MerkleNode(h) for h in hashes]

        while len(nodes) > 1:
            # If odd number of nodes, duplicate the last node
            if len(nodes) % 2 != 0:
                nodes.append(MerkleNode(nodes[-1].hash))

            parent_level = []
            for i in range(0, len(nodes), 2):
                combined = combine_hashes(nodes[i].hash, nodes[i + 1].hash)
                parent_level.append(MerkleNode(combined, left=nodes[i], right=nodes[i + 1]))
            nodes = parent_level

        return nodes[0]

    @property
    def root_hash(self) -> str:
        return self.root.hash if self.root else ""

    def to_dict(self) -> Dict[str, Any]:
        """Serialize tree structure for API and dashboard visualization."""
        return {
            "root_hash": self.root_hash,
            "leaf_count": len(self.leaf_hashes),
            "leaf_hashes": self.leaf_hashes
        }

    @staticmethod
    def find_mismatch_indices(expected_leaves: List[str], actual_leaves: List[str]) -> List[int]:
        """Compare leaf hashes and pinpoint corrupted chunk indices in O(N) or O(log N) tree diff."""
        mismatches = []
        max_len = max(len(expected_leaves), len(actual_leaves))
        for i in range(max_len):
            exp = expected_leaves[i] if i < len(expected_leaves) else None
            act = actual_leaves[i] if i < len(actual_leaves) else None
            if exp != act:
                mismatches.append(i)
        return mismatches
