"""Reed-Solomon (RS 4+2) Erasure Coding Engine.
Implements Galois Field GF(2^8) Cauchy/Vandermonde matrix arithmetic for high-performance chunking,
parity generation, and resilient data reconstruction.

Math specification:
- Field: GF(2^8) with irreducible polynomial 0x11D (x^8 + x^4 + x^3 + x^2 + 1)
- Default setup: K=4 data chunks, M=2 parity chunks (total 6 chunks)
- Fault tolerance: Any 4 surviving chunks out of 6 can reconstruct the original payload.
"""
from typing import List, Tuple, Optional, Dict, Any
import hashlib

# --- Galois Field GF(2^8) Arithmetic ---
GF_POLY = 0x11D  # x^8 + x^4 + x^3 + x^2 + 1
GF_SIZE = 256

gf_exp = [0] * 512
gf_log = [0] * GF_SIZE

def _init_gf_tables():
    """Precompute exponential and logarithm lookup tables for O(1) GF(2^8) multiplication and division."""
    x = 1
    for i in range(255):
        gf_exp[i] = x
        gf_exp[i + 255] = x
        gf_log[x] = i
        x <<= 1
        if x & 0x100:
            x ^= GF_POLY
    gf_log[0] = 0

_init_gf_tables()

def gf_mul(a: int, b: int) -> int:
    """Multiplication in GF(2^8)."""
    if a == 0 or b == 0:
        return 0
    return gf_exp[gf_log[a] + gf_log[b]]

def gf_div(a: int, b: int) -> int:
    """Division in GF(2^8)."""
    if b == 0:
        raise ZeroDivisionError("Division by zero in GF(2^8)")
    if a == 0:
        return 0
    return gf_exp[(gf_log[a] - gf_log[b]) % 255]

def gf_inv(a: int) -> int:
    """Multiplicative inverse in GF(2^8)."""
    if a == 0:
        raise ZeroDivisionError("Zero has no multiplicative inverse in GF(2^8)")
    return gf_exp[255 - gf_log[a]]

# --- Matrix Operations in GF(2^8) ---
def matrix_mul(A: List[List[int]], B: List[List[int]]) -> List[List[int]]:
    """Matrix multiplication in GF(2^8)."""
    rows_A = len(A)
    cols_A = len(A[0])
    rows_B = len(B)
    cols_B = len(B[0])
    if cols_A != rows_B:
        raise ValueError("Incompatible matrix dimensions for multiplication")
    
    C = [[0] * cols_B for _ in range(rows_A)]
    for i in range(rows_A):
        for j in range(cols_B):
            val = 0
            for k in range(cols_A):
                val ^= gf_mul(A[i][k], B[k][j])
            C[i][j] = val
    return C

def matrix_invert(A: List[List[int]]) -> List[List[int]]:
    """Invert a square matrix in GF(2^8) using Gaussian elimination."""
    n = len(A)
    # Augment A with Identity matrix
    augmented = [row[:] + [1 if i == j else 0 for j in range(n)] for i, row in enumerate(A)]

    for i in range(n):
        # Pivot search
        pivot_row = i
        while pivot_row < n and augmented[pivot_row][i] == 0:
            pivot_row += 1
        if pivot_row == n:
            raise ValueError("Matrix is singular and cannot be inverted in GF(2^8)")

        augmented[i], augmented[pivot_row] = augmented[pivot_row], augmented[i]

        # Normalize pivot row
        pivot_inv = gf_inv(augmented[i][i])
        for j in range(2 * n):
            augmented[i][j] = gf_mul(augmented[i][j], pivot_inv)

        # Eliminate column entries in other rows
        for r in range(n):
            if r != i:
                factor = augmented[r][i]
                if factor != 0:
                    for c in range(2 * n):
                        augmented[r][c] ^= gf_mul(augmented[i][c], factor)

    # Extract right-hand side inverse
    inv = [row[n:] for row in augmented]
    return inv

# --- Vandermonde Cauchy Generator Matrix ---
def build_cauchy_matrix(k: int, m: int) -> List[List[int]]:
    """Construct a systematic Cauchy generator matrix [I_k | P] of size (k + m) x k.
    Every square sub-matrix of size k x k is invertible.
    """
    total = k + m
    gen = []
    # Top k rows: Identity matrix
    for i in range(k):
        row = [1 if i == j else 0 for j in range(k)]
        gen.append(row)
    
    # Bottom m rows: Cauchy matrix entries 1 / (X_i ^ Y_j)
    # Choose distinct sets X = {k, k+1, ...}, Y = {0, 1, ..., k-1}
    for i in range(m):
        row = []
        for j in range(k):
            x_val = k + i
            y_val = j
            denom = x_val ^ y_val
            row.append(gf_inv(denom))
        gen.append(row)
    return gen

# --- Erasure Coding Pipeline ---
class Chunk:
    def __init__(self, index: int, chunk_type: str, data: bytes, checksum: str):
        self.index = index            # 0..k-1 (DATA), k..k+m-1 (PARITY)
        self.chunk_type = chunk_type  # 'DATA' or 'PARITY'
        self.data = data
        self.size = len(data)
        self.checksum = checksum      # SHA-256

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "type": self.chunk_type,
            "size": self.size,
            "checksum": self.checksum
        }

class ReedSolomonEngine:
    def __init__(self, data_shards: int = 4, parity_shards: int = 2):
        self.k = data_shards
        self.m = parity_shards
        self.total = self.k + self.m
        self.generator_matrix = build_cauchy_matrix(self.k, self.m)

    def encode(self, data: bytes) -> Tuple[List[Chunk], int]:
        """Split data into K data chunks and generate M parity chunks.
        Returns (list_of_chunks, original_data_size).
        """
        original_size = len(data)
        # Calculate chunk size (padded so that total length is divisible by k)
        chunk_size = (original_size + self.k - 1) // self.k
        if chunk_size == 0:
            chunk_size = 1

        padded_len = chunk_size * self.k
        padded_data = data + b'\x00' * (padded_len - original_size)

        # 1. Extract K data chunks
        data_chunks = []
        for i in range(self.k):
            start = i * chunk_size
            chunk_bytes = padded_data[start:start + chunk_size]
            c_hash = hashlib.sha256(chunk_bytes).hexdigest()
            data_chunks.append(Chunk(i, "DATA", chunk_bytes, c_hash))

        # 2. Compute M parity chunks via Cauchy matrix multiplication
        # P[i] = sum_{j=0}^{k-1} Cauchy[i][j] * D[j]
        all_chunks = list(data_chunks)
        for i in range(self.m):
            parity_row = self.generator_matrix[self.k + i]
            parity_bytes = bytearray(chunk_size)
            for byte_idx in range(chunk_size):
                val = 0
                for j in range(self.k):
                    coeff = parity_row[j]
                    d_byte = data_chunks[j].data[byte_idx]
                    val ^= gf_mul(coeff, d_byte)
                parity_bytes[byte_idx] = val
            
            p_bytes = bytes(parity_bytes)
            p_hash = hashlib.sha256(p_bytes).hexdigest()
            all_chunks.append(Chunk(self.k + i, "PARITY", p_bytes, p_hash))

        return all_chunks, original_size

    def decode(self, available_chunks: List[Chunk], original_size: int) -> bytes:
        """Reconstruct original payload using ANY K surviving chunks out of K+M.
        Raises ValueError if fewer than K chunks are available.
        """
        if len(available_chunks) < self.k:
            raise ValueError(f"Insufficient shards for reconstruction: need {self.k}, got {len(available_chunks)}")

        # Select first K distinct valid chunks
        selected = available_chunks[:self.k]
        chunk_size = len(selected[0].data)

        # Check if we already have all K data chunks intact (Fast path!)
        data_chunks_map = {c.index: c.data for c in selected if c.index < self.k}
        if len(data_chunks_map) == self.k:
            full_data = b"".join(data_chunks_map[i] for i in range(self.k))
            return full_data[:original_size]

        # Reconstruct using submatrix inversion
        # Build K x K submatrix corresponding to the rows of selected chunks
        sub_matrix = [self.generator_matrix[c.index] for c in selected]
        inv_matrix = matrix_invert(sub_matrix)

        # Multiply inv_matrix with available chunks to restore original data chunks D
        reconstructed_data_chunks = [bytearray(chunk_size) for _ in range(self.k)]
        for byte_idx in range(chunk_size):
            for i in range(self.k):
                val = 0
                for j in range(self.k):
                    coeff = inv_matrix[i][j]
                    s_byte = selected[j].data[byte_idx]
                    val ^= gf_mul(coeff, s_byte)
                reconstructed_data_chunks[i][byte_idx] = val

        full_data = b"".join(bytes(buf) for buf in reconstructed_data_chunks)
        return full_data[:original_size]

    def reconstruct_lost_chunk(self, available_chunks: List[Chunk], target_chunk_index: int) -> Chunk:
        """Reconstruct a specific lost or corrupted chunk (Data or Parity) from K surviving chunks."""
        if len(available_chunks) < self.k:
            raise ValueError(f"Need at least {self.k} surviving chunks to heal lost chunk {target_chunk_index}")

        # First decode all original data chunks
        selected = available_chunks[:self.k]
        chunk_size = len(selected[0].data)
        
        # If target is a data chunk, decode all data
        sub_matrix = [self.generator_matrix[c.index] for c in selected]
        inv_matrix = matrix_invert(sub_matrix)

        if target_chunk_index < self.k:
            # Reconstruct target data chunk directly
            target_buf = bytearray(chunk_size)
            for byte_idx in range(chunk_size):
                val = 0
                for j in range(self.k):
                    coeff = inv_matrix[target_chunk_index][j]
                    s_byte = selected[j].data[byte_idx]
                    val ^= gf_mul(coeff, s_byte)
                target_buf[byte_idx] = val
            
            repaired_bytes = bytes(target_buf)
            checksum = hashlib.sha256(repaired_bytes).hexdigest()
            return Chunk(target_chunk_index, "DATA", repaired_bytes, checksum)
        else:
            # Target is a parity chunk: reconstruct all K data chunks, then compute target parity
            reconstructed_data = [bytearray(chunk_size) for _ in range(self.k)]
            for byte_idx in range(chunk_size):
                for i in range(self.k):
                    val = 0
                    for j in range(self.k):
                        val ^= gf_mul(inv_matrix[i][j], selected[j].data[byte_idx])
                    reconstructed_data[i][byte_idx] = val

            parity_row = self.generator_matrix[target_chunk_index]
            target_buf = bytearray(chunk_size)
            for byte_idx in range(chunk_size):
                val = 0
                for j in range(self.k):
                    coeff = parity_row[j]
                    d_byte = reconstructed_data[j][byte_idx]
                    val ^= gf_mul(coeff, d_byte)
                target_buf[byte_idx] = val

            repaired_bytes = bytes(target_buf)
            checksum = hashlib.sha256(repaired_bytes).hexdigest()
            return Chunk(target_chunk_index, "PARITY", repaired_bytes, checksum)

erasure_coding_engine = ReedSolomonEngine(data_shards=4, parity_shards=2)
