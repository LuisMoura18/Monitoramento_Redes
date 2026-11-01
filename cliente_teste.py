"""
Cliente de teste em linha de comando (só biblioteca padrão).

Serve para testar o servidor sem navegador e como base para os scripts de medição.

Exemplos:
  python3 cliente_teste.py --usuario bot1 --senha 1234 chat            # conversa pelo terminal
  python3 cliente_teste.py --usuario bot1 --senha 1234 rtt -n 30       # RTT de aplicação (ping/pong WS)
  python3 cliente_teste.py --usuario bot1 --senha 1234 enviar "oi" -n 5 --intervalo 0.5
  python3 cliente_teste.py --host 192.168.0.10 --usuario bot2 --senha 1234 rtt -n 30
"""
import argparse
import asyncio
import json
import statistics
import sys
import time
import http.client

import wsproto as ws


def login(host, port, usuario, senha):
    con = http.client.HTTPConnection(host, port, timeout=10)
    con.request("POST", "/api/login", json.dumps({"usuario": usuario, "senha": senha}),
                {"Content-Type": "application/json"})
    r = con.getresponse()
    corpo = json.loads(r.read() or b"{}")
    con.close()
    if r.status not in (200, 201):
        sys.exit(f"login falhou: {r.status} {corpo}")
    return corpo["token"]


class ClienteWS:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    @classmethod
    async def conectar(cls, host, port, token, sala="geral"):
        reader, writer = await asyncio.open_connection(host, port)
        chave = ws.new_client_key()
        writer.write(
            (f"GET /ws?token={token}&sala={sala} HTTP/1.1\r\nHost: {host}:{port}\r\n"
             "Upgrade: websocket\r\nConnection: Upgrade\r\n"
             f"Sec-WebSocket-Key: {chave}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        await writer.drain()
        resp = (await reader.readuntil(b"\r\n\r\n")).decode()
        if " 101 " not in resp.split("\r\n")[0]:
            raise ConnectionError("handshake recusado:\n" + resp)
        if ws.accept_key(chave) not in resp:
            raise ConnectionError("Sec-WebSocket-Accept incorreto")
        return cls(reader, writer)

    async def enviar(self, obj):
        self.writer.write(ws.encode_frame(json.dumps(obj).encode(), mask=True))
        await self.writer.drain()

    async def receber(self):
        """Devolve o próximo objeto JSON (responde pings do servidor automaticamente)."""
        while True:
            op, dados, _ = await ws.read_message(self.reader, 4 * 1024 * 1024)
            if op == ws.OP_PING:
                self.writer.write(ws.encode_frame(dados, ws.OP_PONG, mask=True))
                continue
            if op == ws.OP_CLOSE:
                raise ConnectionError("servidor fechou a conexão")
            if op == ws.OP_TEXT:
                return json.loads(dados)

    async def esperar_tipo(self, tipo, timeout=5):
        async def laco():
            while True:
                m = await self.receber()
                if m.get("tipo") == tipo:
                    return m
        return await asyncio.wait_for(laco(), timeout)

    async def fechar(self):
        try:
            self.writer.write(ws.encode_close(1000, mask=True))
            await self.writer.drain()
            self.writer.close()
        except Exception:
            pass


async def modo_rtt(c, n, intervalo):
    rtts = []
    for i in range(1, n + 1):
        t0 = time.perf_counter()
        await c.enviar({"tipo": "ping", "seq": i, "t": time.time()})
        while True:
            m = await c.esperar_tipo("pong")
            if m.get("seq") == i:
                break
        rtts.append((time.perf_counter() - t0) * 1000)
        await asyncio.sleep(intervalo)
    print(f"amostras={len(rtts)}  média={statistics.mean(rtts):.3f} ms  mediana={statistics.median(rtts):.3f} ms  "
          f"desvio={statistics.stdev(rtts) if len(rtts) > 1 else 0:.3f} ms  min={min(rtts):.3f}  max={max(rtts):.3f}")
    return rtts


async def modo_enviar(c, texto, n, intervalo):
    for i in range(n):
        await c.enviar({"tipo": "msg", "id_cliente": f"t{i}", "conteudo": f"{texto} #{i + 1}" if n > 1 else texto})
        while True:
            m = await c.receber()
            if m.get("tipo") in ("ack", "erro"):
                print(m)
                break
        await asyncio.sleep(intervalo)


async def modo_chat(c):
    print("conectado. digite mensagens e Enter (Ctrl+C sai)")
    loop = asyncio.get_running_loop()

    async def ler_rede():
        while True:
            m = await c.receber()
            if m["tipo"] == "msg":
                print(f"\r[{m['ts'][11:19]}] {m['usuario']}: {m['conteudo']}")
            elif m["tipo"] in ("alerta", "erro", "presenca"):
                print(f"\r<{m['tipo']}> {m}")

    tarefa = asyncio.create_task(ler_rede())
    while True:
        linha = await loop.run_in_executor(None, sys.stdin.readline)
        if not linha:
            break
        if linha.strip():
            await c.enviar({"tipo": "msg", "id_cliente": str(time.time()), "conteudo": linha.strip()})
    tarefa.cancel()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--usuario", required=True)
    ap.add_argument("--senha", required=True)
    ap.add_argument("--sala", default="geral")
    sub = ap.add_subparsers(dest="modo", required=True)
    r = sub.add_parser("rtt"); r.add_argument("-n", type=int, default=30); r.add_argument("--intervalo", type=float, default=0.1)
    e = sub.add_parser("enviar"); e.add_argument("texto"); e.add_argument("-n", type=int, default=1); e.add_argument("--intervalo", type=float, default=0.2)
    sub.add_parser("chat")
    a = ap.parse_args()

    token = login(a.host, a.port, a.usuario, a.senha)
    c = await ClienteWS.conectar(a.host, a.port, token, a.sala)
    await c.esperar_tipo("bem_vindo")
    try:
        if a.modo == "rtt":
            await modo_rtt(c, a.n, a.intervalo)
        elif a.modo == "enviar":
            await modo_enviar(c, a.texto, a.n, a.intervalo)
        else:
            await modo_chat(c)
    finally:
        await c.fechar()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
