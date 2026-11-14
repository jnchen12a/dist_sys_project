"""Fixed-size UTF-8 records and Cauchy codes over an automatically sized GF(2^w)."""

from dataclasses import dataclass
from functools import cache
import struct


@dataclass(frozen=True)
class Record:
    key: str
    value: str


class RecordCodec:
    def __init__(self, max_key_bytes=64, max_value_bytes=256):
        if (type(max_key_bytes) is not int or type(max_value_bytes) is not int
                or not 0 <= max_key_bytes < 2**32
                or not 0 <= max_value_bytes < 2**32):
            raise ValueError("Record limits must be nonnegative 32-bit integers")
        self.max_key_bytes = max_key_bytes
        self.max_value_bytes = max_value_bytes
        self.size = 9 + max_key_bytes + max_value_bytes
        self.empty = bytes(self.size)

    def validate_key(self, key):
        if not isinstance(key, str):
            raise TypeError("Keys must be strings")
        if len(key.encode("utf-8")) > self.max_key_bytes:
            raise ValueError("Key exceeds maximum UTF-8 byte length")

    def encode(self, record):
        if record is None:
            return self.empty
        self.validate_key(record.key)
        if not isinstance(record.value, str):
            raise TypeError("Values must be strings")
        key, value = record.key.encode("utf-8"), record.value.encode("utf-8")
        if len(value) > self.max_value_bytes:
            raise ValueError("Value exceeds maximum UTF-8 byte length")
        payload = struct.pack(">BII", 1, len(key), len(value)) + key + value
        return payload + bytes(self.size - len(payload))

    def decode(self, encoded):
        if not isinstance(encoded, bytes) or len(encoded) != self.size:
            raise ValueError("Wrong encoded record size or type")
        if encoded == self.empty:
            return None
        occupied, key_length, value_length = struct.unpack(">BII", encoded[:9])
        if (occupied != 1 or key_length > self.max_key_bytes
                or value_length > self.max_value_bytes):
            raise ValueError("Invalid occupancy or record lengths")
        end_key = 9 + key_length
        end_value = end_key + value_length
        try:
            record = Record(encoded[9:end_key].decode("utf-8"),
                            encoded[end_key:end_value].decode("utf-8"))
        except UnicodeDecodeError as error:
            raise ValueError("Invalid UTF-8 in record") from error
        if any(encoded[end_value:]):
            raise ValueError("Record padding must be zero")
        return record


# x^8 + x^4 + x^3 + x^2 + 1, with primitive element 2.
_EXP = [0] * 510
_LOG = [0] * 256
_value = 1
for _index in range(255):
    _EXP[_index] = _value
    _LOG[_value] = _index
    _value <<= 1
    if _value & 256:
        _value ^= 0x11D
for _index in range(255, 510):
    _EXP[_index] = _EXP[_index - 255]


def multiply(a, b):
    return 0 if a == 0 or b == 0 else _EXP[_LOG[a] + _LOG[b]]


def inverse(value):
    if value == 0:
        raise ValueError("Zero has no multiplicative inverse")
    return _EXP[255 - _LOG[value]]


def xor_bytes(left, right):
    if len(left) != len(right):
        raise ValueError("Mismatched record sizes")
    return bytes(a ^ b for a, b in zip(left, right))


def _polynomial_remainder(value, divisor):
    while value.bit_length() >= divisor.bit_length():
        value ^= divisor << (value.bit_length() - divisor.bit_length())
    return value


def _polynomial_gcd(left, right):
    while right:
        left, right = right, _polynomial_remainder(left, right)
    return left


def _irreducible(polynomial, bits):
    power = 2  # The polynomial x.
    for exponent in range(1, bits + 1):
        square = sum(((power >> i) & 1) << (2 * i) for i in range(power.bit_length()))
        power = _polynomial_remainder(square, polynomial)
        if exponent <= bits // 2 and _polynomial_gcd(power ^ 2, polynomial) != 1:
            return False
    return power == 2


@cache
def _field_polynomial(bits):
    if bits == 8:
        return 0x11D
    candidate = (1 << bits) | 1
    while not _irreducible(candidate, bits):
        candidate += 2
    return candidate


class BinaryField:
    def __init__(self, bits):
        self.bits = bits
        self.polynomial = _field_polynomial(bits)
        self.inverses = {}

    def multiply(self, left, right):
        if self.bits == 8:
            return multiply(left, right)
        result = 0
        while right:
            if right & 1:
                result ^= left
            right >>= 1
            left <<= 1
            if left & (1 << self.bits):
                left ^= self.polynomial
        return result

    def inverse(self, value):
        if self.bits == 8:
            return inverse(value)
        if value == 0:
            raise ValueError("Zero has no multiplicative inverse")
        if value not in self.inverses:
            result, base, exponent = 1, value, (1 << self.bits) - 2
            while exponent:
                if exponent & 1:
                    result = self.multiply(result, base)
                base = self.multiply(base, base)
                exponent >>= 1
            self.inverses[value] = result
        return self.inverses[value]


def linear_combination(coefficients, records, size, field=None,
                       input_symbol_bytes=1, output_symbol_bytes=1):
    if len(coefficients) != len(records):
        raise ValueError("Coefficient and record counts differ")
    multiply_value = multiply if field is None else field.multiply
    result = [0] * size
    for coefficient, record in zip(coefficients, records):
        if len(record) != size * input_symbol_bytes:
            raise ValueError("Mismatched record sizes")
        symbols = record if input_symbol_bytes == 1 else [
            int.from_bytes(record[offset:offset + input_symbol_bytes], "big")
            for offset in range(0, len(record), input_symbol_bytes)
        ]
        for offset, value in enumerate(symbols):
            result[offset] ^= multiply_value(coefficient, value)
    if output_symbol_bytes == 1:
        return bytes(result)
    return b"".join(value.to_bytes(output_symbol_bytes, "big") for value in result)


def invert_matrix(matrix, field=None):
    multiply_value = multiply if field is None else field.multiply
    inverse_value = inverse if field is None else field.inverse
    size = len(matrix)
    if not size or any(len(row) != size for row in matrix):
        raise ValueError("Expected a nonempty square matrix")
    rows = [list(row) + [int(i == j) for j in range(size)]
            for i, row in enumerate(matrix)]
    for column in range(size):
        pivot = next((i for i in range(column, size) if rows[i][column]), None)
        if pivot is None:
            raise ValueError("Singular coding matrix")
        rows[column], rows[pivot] = rows[pivot], rows[column]
        scale = inverse_value(rows[column][column])
        rows[column] = [multiply_value(scale, value) for value in rows[column]]
        for i in range(size):
            if i != column and rows[i][column]:
                factor = rows[i][column]
                rows[i] = [a ^ multiply_value(factor, b)
                           for a, b in zip(rows[i], rows[column])]
    return [row[size:] for row in rows]


class CauchyCode:
    def __init__(self, n, k, record_size):
        if (type(n) is not int or type(k) is not int
                or not 1 <= k < n):
            raise ValueError("Coding requires 1 <= k < n")
        if type(record_size) is not int or record_size < 1:
            raise ValueError("Record size must be positive")
        self.n, self.k, self.size = n, k, record_size
        self.field = BinaryField(max(8, (n + k - 1).bit_length()))
        self.symbol_bytes = (self.field.bits + 7) // 8
        self.parity_size = self.size * self.symbol_bytes
        self.empty = bytes(self.parity_size)
        # Disjoint x={0,...,k-1}, y={k,...,k+n-1}. Every square
        # submatrix of this Cauchy matrix is nonsingular.
        self.matrix = [[self.field.inverse(p ^ (k + i)) for i in range(n)] for p in range(k)]

    def encode(self, records):
        if len(records) != self.n:
            raise ValueError("Expected one record per primary")
        return [linear_combination(row, records, self.size, self.field,
                                   output_symbol_bytes=self.symbol_bytes) for row in self.matrix]

    def update(self, parity, stream, primary, old, new):
        delta = xor_bytes(old, new)
        contribution = linear_combination([self.matrix[stream][primary]],
                                          [delta], self.size, self.field,
                                          output_symbol_bytes=self.symbol_bytes)
        return xor_bytes(parity, contribution)

    def decoder(self, missing, streams):
        if not 1 <= len(missing) <= self.k or len(streams) != len(missing):
            raise ValueError("Need one parity equation per missing primary")
        return invert_matrix([[self.matrix[p][i] for i in missing] for p in streams], self.field)

    def recover(self, surviving, parity, missing, streams=None, decoder=None):
        """surviving maps primary index to bytes; parity maps stream to bytes."""
        missing = tuple(missing)
        streams = tuple(sorted(parity)[:len(missing)] if streams is None else streams)
        if (set(surviving) != set(range(self.n)) - set(missing)
                or len(set(missing)) != len(missing)):
            raise ValueError("Missing and surviving primaries must partition the ring")
        decoder = self.decoder(missing, streams) if decoder is None else decoder
        survivors = sorted(surviving)
        residuals = []
        for p in streams:
            known = linear_combination([self.matrix[p][i] for i in survivors],
                                       [surviving[i] for i in survivors], self.size, self.field,
                                       output_symbol_bytes=self.symbol_bytes)
            residuals.append(xor_bytes(parity[p], known))
        return {i: linear_combination(row, residuals, self.size, self.field,
                                     input_symbol_bytes=self.symbol_bytes)
                for i, row in zip(missing, decoder)}
