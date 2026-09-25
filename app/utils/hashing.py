"""Hashing and byte manipulation utilities."""
import hashlib
from pathlib import Path
from typing import Tuple, Union

def calculate_sha256(data: bytes) -> str:
    """Calculate hex SHA-256 checksum for given bytes."""
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()

def calculate_file_sha256(file_path: Union[str, Path], chunk_size: int = 65536) -> Tuple[str, int]:
    """Calculate SHA-256 checksum and total byte size of a file on disk."""
    hasher = hashlib.sha256()
    total_bytes = 0
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
            total_bytes += len(chunk)
            
    return hasher.hexdigest(), total_bytes

def verify_checksum(actual: str, expected: str) -> bool:
    """Case-insensitive checksum comparison."""
    if not actual or not expected:
        return False
    return actual.strip().lower() == expected.strip().lower()

def format_bytes(num_bytes: int) -> str:
    """Format bytes count into human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:3.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"
