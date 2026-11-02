"""
Experimento C: 1, 2 e 5 clientes simultaneos.

Cada cliente manda PINGS em sequencia (espera o pong, manda o proximo).
Mede o atraso de cada ping (RTT) e a taxa de mensagens (pongs por segundo, somando todos).

Uso (na mesma pasta do cliente_teste.py):
  python3 concorrencia.py                        # localhost
  python3 concorrencia.py --host 192.168.0.10    # outra maquina
"""
import argparse
import asyncio
import statistics
import time

from cliente_teste import ClienteWS, login


async def rodar_cliente(c, n_pings):
    atrasos = []
    for _ in range(n_pings):
        t0 = time.perf_counter()
        await c.enviar({"tipo": "ping", "t": 0})
        await c.esperar_tipo("pong", timeout=10)
        atrasos.append((time.perf_counter() - t0) * 1000)
    return atrasos


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--senha", default="1234")
    ap.add_argument("-n", "--pings", type=int, default=200, help="pings por cliente (padrao 200)")
    a = ap.parse_args()

    print(f"{a.host}:{a.port} | {a.pings} pings por cliente\n")
    print(f"{'clientes':>8}{'pings':>8}{'media(ms)':>11}{'mediana(ms)':>13}{'desvio(ms)':>12}{'taxa(msg/s)':>13}")

    for n in (1, 2, 5):
        clientes = []
        for i in range(n):
            token = login(a.host, a.port, f"bot_c{i + 1}", a.senha)
            c = await ClienteWS.conectar(a.host, a.port, token)
            await c.esperar_tipo("bem_vindo")
            clientes.append(c)

        t0 = time.perf_counter()
        listas = await asyncio.gather(*(rodar_cliente(c, a.pings) for c in clientes))
        total_s = time.perf_counter() - t0

        atrasos = [x for lista in listas for x in lista]
        print(f"{n:>8}{len(atrasos):>8}{statistics.mean(atrasos):>11.2f}"
              f"{statistics.median(atrasos):>13.2f}{statistics.stdev(atrasos):>12.2f}"
              f"{len(atrasos) / total_s:>13.0f}")

        for c in clientes:
            await c.fechar()
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())