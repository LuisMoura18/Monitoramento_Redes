"""
Experimento B: throughput com tamanhos de mensagem diferentes.
Uso:
  python3 throughput.py                              # localhost
  python3 throughput.py --host 192.168.0.10          # outra maquina
  python3 throughput.py --host 192.168.0.10 -r 10    # 10 repeticoes por tamanho
"""
import argparse
import asyncio
import http.client
import json
import statistics
import time

from cliente_teste import ClienteWS, login

#tamanho de conteudo em bytes
TAMANHOS = [1_000, 10_000, 100_000, 500_000, 1_000_000, 2_000_000]


def mbps(n_bytes: int, segundos: float) -> float:
    return n_bytes * 8 / segundos / 1e6

#devolve o tempo total para enviar n bytes e receber o pong
async def medir_ws(c: ClienteWS, n: int, seq: int) -> float:
    msg = {"tipo": "ping", "seq": seq, "pad": "x" * n}
    t0 = time.perf_counter()
    await c.enviar(msg)
    while True:
        m = await c.esperar_tipo("pong", timeout=30)
        if m.get("seq") == seq:
            break
    return time.perf_counter() - t0


def medir_http(con: http.client.HTTPConnection, token: str, n: int) -> float:
    corpo = b"x" * n
    t0 = time.perf_counter()
    con.request("POST", "/api/eco", corpo,
                {"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"})
    r = con.getresponse()
    dados = json.loads(r.read())
    dt = time.perf_counter() - t0
    if r.status != 200 or dados.get("recebidos_bytes") != n:
        raise RuntimeError(f"resposta inesperada: {r.status} {dados}")
    return dt


def resumo(nome: str, n: int, tempos: list[float]):
    vazoes = [mbps(n, t) for t in tempos]
    dp = statistics.stdev(vazoes) if len(vazoes) > 1 else 0.0
    print(f"{nome:<10}{n:>10,}{len(tempos):>6}"
          f"{statistics.mean(tempos) * 1000:>14.2f}"
          f"{statistics.mean(vazoes):>16.2f}{dp:>12.2f}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--usuario", default="bot_tp")
    ap.add_argument("--senha", default="1234")
    ap.add_argument("-r", "--repeticoes", type=int, default=5, help="repeticoes por tamanho (padrao 5)")
    a = ap.parse_args()

    token = login(a.host, a.port, a.usuario, a.senha)
    c = await ClienteWS.conectar(a.host, a.port, token)
    await c.esperar_tipo("bem_vindo")
    con = http.client.HTTPConnection(a.host, a.port, timeout=30)

    #descarta a primeira transferencia
    await medir_ws(c, 10_000, 0)
    medir_http(con, token, 10_000)

    print(f"servidor {a.host}:{a.port} | {a.repeticoes} repeticoes por tamanho | upload cliente->servidor\n")
    print(f"{'canal':<10}{'bytes':>10}{'reps':>6}{'tempo(ms)':>14}{'Mbit/s medio':>16}{'desvio':>12}")
    print("-" * 68)

    seq = 1
    for n in TAMANHOS:
        tempos = []
        for _ in range(a.repeticoes):
            tempos.append(await medir_ws(c, n, seq))
            seq += 1
        resumo("WebSocket", n, tempos)
    print("-" * 68)
    for n in TAMANHOS:
        tempos = [medir_http(con, token, n) for _ in range(a.repeticoes)]
        resumo("HTTP", n, tempos)

    con.close()
    await c.fechar()


if __name__ == "__main__":
    asyncio.run(main())