"""HyperLogLog: a count-distinct that COMPOSES, in about eighty lines and no dependency (chunk 88).

Why this exists. "How many different items were picked this month" is not a sum of daily answers: an
item picked on twelve days is one item. Every other rollup role is additive - sums add, counts add,
histograms add element-wise - and that additivity is what lets a month be folded from its days. An
exact set would compose too, but it is unbounded: one busy warehouse day could hold thousands of item
numbers in one row. A sketch is a fixed 4 KB that unions register by register with `max`, so it slots
into the same cascade as every other role, at the price of being an ESTIMATE.

The arithmetic, stated so it can be checked against the paper (Flajolet et al., 2007) without reading
the code: hash each value to 64 bits; the top `PRECISION` bits pick one of `REGISTERS` registers; the
position of the leftmost 1 in the remaining bits is the observation, and a register keeps the maximum
it has seen. A register value of r says "I have seen a run of r-1 leading zeros", which happens once
in 2^r values, so the harmonic mean of 2^-register across all registers estimates the cardinality.
Small sets are corrected by linear counting on the number of still-empty registers, which is far more
accurate there. No large-range correction: with a 64-bit hash, that regime is beyond any real count.

Precision 12 is 4096 registers, about 1.6 percent standard error. Registers are stored as `bytes`, one
byte each, so the column is a plain `bytea` and a sketch is a value: hashable, comparable, a pure
function of the set of values it has seen. That last property is what the tests pin.

In-repo rather than a library, decided 2026-09-12: the server has no hll extension, the maths fits on
one screen, and a dependency for eighty lines is a maintenance liability with no upside.
"""

import hashlib
import math
from typing import Iterable

#: Number of hash bits used to choose a register. 12 -> 4096 registers -> ~1.6 percent error, 4 KB.
PRECISION = 12
REGISTERS = 1 << PRECISION
_HASH_BITS = 64
_REST_BITS = _HASH_BITS - PRECISION
_REST_MASK = (1 << _REST_BITS) - 1
#: The bias-correcting constant alpha_m for m >= 128, from the paper.
_ALPHA = 0.7213 / (1 + 1.079 / REGISTERS)

#: The sketch of no values. A value, so `estimate(EMPTY) == 0` and `union(x, EMPTY) == x`.
EMPTY: bytes = bytes(REGISTERS)


def _hash64(value: str) -> int:
    """A stable 64-bit hash. blake2b rather than `hash()`: Python's is salted per process, and a sketch
    written by one worker must be readable by the next."""
    return int.from_bytes(hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest(), "big")


def add(sketch: bytes, value: str) -> bytes:
    """`sketch` with `value` observed. Idempotent: adding a value already seen changes nothing."""
    h = _hash64(value)
    index = h >> _REST_BITS
    rest = h & _REST_MASK
    # Leading zeros in the remaining bits, plus one. All-zero rest is the longest run possible.
    rho = _REST_BITS - rest.bit_length() + 1
    if sketch[index] >= rho:
        return sketch
    out = bytearray(sketch)
    out[index] = rho
    return bytes(out)


def from_values(values: Iterable[str]) -> bytes:
    """The sketch of every value in `values`. Order does not matter."""
    registers = bytearray(REGISTERS)
    for value in values:
        h = _hash64(value)
        index = h >> _REST_BITS
        rho = _REST_BITS - (h & _REST_MASK).bit_length() + 1
        if registers[index] < rho:
            registers[index] = rho
    return bytes(registers)


def union(a: bytes, b: bytes) -> bytes:
    """The sketch of the union of two sets: register-wise maximum. Commutative, associative, and
    idempotent, which is exactly what the grain cascade needs of an additive role."""
    if a == EMPTY:
        return b
    if b == EMPTY:
        return a
    return bytes(max(x, y) for x, y in zip(a, b))


def estimate(sketch: bytes) -> int:
    """The estimated number of distinct values `sketch` has seen, rounded to an integer."""
    if not sketch or sketch == EMPTY:
        return 0
    zeros = 0
    inverse_sum = 0.0
    for register in sketch:
        if register == 0:
            zeros += 1
        inverse_sum += 2.0 ** -register
    raw = _ALPHA * REGISTERS * REGISTERS / inverse_sum
    if raw <= 2.5 * REGISTERS and zeros:
        # Small range: linear counting on the empty registers is the better estimator here.
        return round(REGISTERS * math.log(REGISTERS / zeros))
    return round(raw)
