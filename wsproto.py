"""
Implementação mínima do protocolo WebSocket (RFC 6455) usando só a biblioteca padrão.

Usada pelo servidor (server.py) e pelo cliente de teste (cliente_teste.py).
Cobre: handshake, quadros de texto/binário, fragmentação, ping/pong e close.
Não implementa extensões (ex.: permessage-deflate).
"""
import base64
import hashlib
import os
import struct

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT = 0x0
OP_TEXT = 0x1
OP_BIN = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


class FrameTooLarge(Exception):
    """Quadro ou mensagem maior que o limite configurado."""


class ProtocolError(Exception):
    """Quadro malformado ou que viola a RFC 6455."""


def accept_key(client_key: str) -> str:
    """Calcula Sec-WebSocket-Accept a partir de Sec-WebSocket-Key."""
    digest = hashlib.sha1((client_key + GUID).encode()).digest()
    return base64.b64encode(digest).decode()


def new_client_key() -> str:
    return base64.b64encode(os.urandom(16)).decode()


def _mask(data: bytes, key: bytes) -> bytes:
    """Aplica (ou remove) a máscara XOR de 4 bytes. Usa inteiros grandes para ser rápido."""
    n = len(data)
    if n == 0:
        return data
    k = (key * (n // 4 + 1))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(k, "big")).to_bytes(n, "big")


def encode_frame(payload: bytes, opcode: int = OP_TEXT, mask: bool = False) -> bytes:
    """Monta um quadro único (FIN=1). Clientes DEVEM mascarar; servidores NÃO."""
    b0 = 0x80 | opcode
    n = len(payload)
    mbit = 0x80 if mask else 0
    if n < 126:
        header = struct.pack("!BB", b0, mbit | n)
    elif n < 65536:
        header = struct.pack("!BBH", b0, mbit | 126, n)
    else:
        header = struct.pack("!BBQ", b0, mbit | 127, n)
    if mask:
        key = os.urandom(4)
        return header + key + _mask(payload, key)
    return header + payload


def encode_close(code: int = 1000, reason: str = "", mask: bool = False) -> bytes:
    return encode_frame(struct.pack("!H", code) + reason.encode()[:120], OP_CLOSE, mask)


async def read_frame(reader, max_size: int):
    """Lê um quadro. Retorna (fin, opcode, payload, mascarado)."""
    h = await reader.readexactly(2)
    fin = bool(h[0] & 0x80)
    if h[0] & 0x70:
        raise ProtocolError("bits RSV não suportados")
    opcode = h[0] & 0x0F
    masked = bool(h[1] & 0x80)
    n = h[1] & 0x7F
    if n == 126:
        n = struct.unpack("!H", await reader.readexactly(2))[0]
    elif n == 127:
        n = struct.unpack("!Q", await reader.readexactly(8))[0]
    if opcode >= 0x8 and (n > 125 or not fin):
        raise ProtocolError("quadro de controle inválido")
    if n > max_size:
        raise FrameTooLarge(n)
    key = await reader.readexactly(4) if masked else None
    data = await reader.readexactly(n)
    if key:
        data = _mask(data, key)
    return fin, opcode, data, masked


async def read_message(reader, max_size: int):
    """
    Lê uma mensagem completa, juntando fragmentos.
    Quadros de controle (ping/pong/close) são devolvidos assim que chegam.
    Retorna (opcode, payload, mascarado).
    """
    buf = bytearray()
    first_op = None
    while True:
        fin, op, data, masked = await read_frame(reader, max_size)
        if op >= 0x8:
            return op, data, masked
        if op == OP_CONT:
            if first_op is None:
                raise ProtocolError("continuação sem quadro inicial")
            buf += data
        else:
            first_op = op
            buf = bytearray(data)
        if len(buf) > max_size:
            raise FrameTooLarge(len(buf))
        if fin:
            return first_op, bytes(buf), masked
