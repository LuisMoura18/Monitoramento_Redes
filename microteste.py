"""
Experimento D: microteste TCP vs UDP (mesma porta, mesma carga).

O cliente envia N mensagens de TAMANHO bytes primeiro por TCP e depois por UDP, o mais
rapido possivel. Cada mensagem leva um cabecalho de 12 bytes (numero de sequencia, total e
tamanho). O servidor confere o que chegou: perdas, ordem e vazao.

    python3 tcp_udp.py servidor                         #servidor
    python3 tcp_udp.py cliente --host IP_DO_SERVIDOR    #cliente
  (use --n e --tamanho iguais nas duas execucoes do cliente; tamanho entre 12 e 1472)
"""
import argparse
import socket
import struct
import time

PORTA = 9000
HDR = struct.Struct("!III")  # sequencia, total de mensagens, tam da mensagem


class Contador:
    def __init__(self):
        self.vistos, self.fora, self.maior = set(), 0, -1
        self.total = self.tam = 0
        self.t0 = self.t1 = None

    def registrar(self, seq, total, tam):
        agora = time.perf_counter()
        self.t0 = self.t0 or agora
        self.t1 = agora
        self.total, self.tam = total, tam
        if seq < self.maior:
            self.fora += 1          #chegou depois de uma mensagem mais nova
        self.maior = max(self.maior, seq)
        self.vistos.add(seq)

    def mostrar(self, nome):
        rec, dt = len(self.vistos), max(self.t1 - self.t0, 1e-9)
        perd = self.total - rec
        print(f"{nome}: recebidas {rec}/{self.total} | perdidas {perd} ({100 * perd / self.total:.1f}%) | "
              f"fora de ordem {self.fora} | {rec * self.tam / 1e6:.2f} MB em {dt:.3f} s "
              f"= {rec * self.tam * 8 / dt / 1e6:.1f} Mbit/s")


def ler(con, n):
    buf = b""
    while len(buf) < n:
        parte = con.recv(n - len(buf))
        if not parte:
            return None
        buf += parte
    return buf


def servidor():
    tcp = socket.socket()
    tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp.bind(("", PORTA))
    tcp.listen(1)
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("", PORTA))
    print(f"servidor ouvindo TCP e UDP na porta {PORTA}. Ctrl+C para sair.\n")
    while True:
        #TCP le ate o cliente fechar a conexao
        con, _ = tcp.accept()
        c = Contador()
        while (h := ler(con, HDR.size)) is not None:
            seq, total, tam = HDR.unpack(h)
            ler(con, tam - HDR.size)
            c.registrar(seq, total, tam)
        con.close()
        c.mostrar("TCP")
        # UDP le ate ficar 2 s sem receber nada
        c = Contador()
        udp.settimeout(None)
        while True:
            try:
                dados, _ = udp.recvfrom(65535)
            except socket.timeout:
                break
            udp.settimeout(2)
            c.registrar(*HDR.unpack(dados[:HDR.size]))
        c.mostrar("UDP")
        print()


def cliente(host, n, tam):
    pad = bytes(tam - HDR.size)
    s = socket.create_connection((host, PORTA))
    t0 = time.perf_counter()
    for i in range(n):
        s.sendall(HDR.pack(i, n, tam) + pad)
    s.close()
    print(f"TCP: {n} mensagens de {tam} B enviadas em {time.perf_counter() - t0:.3f} s")
    time.sleep(0.5)
    u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t0 = time.perf_counter()
    for i in range(n):
        u.sendto(HDR.pack(i, n, tam) + pad, (host, PORTA))
    print(f"UDP: {n} mensagens de {tam} B enviadas em {time.perf_counter() - t0:.3f} s")
    print("Veja o resultado (recebidas, perdas, ordem) no terminal do servidor.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("modo", choices=["servidor", "cliente"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--n", type=int, default=2000, help="numero de mensagens (padrao 2000)")
    ap.add_argument("--tamanho", type=int, default=1000, help="bytes por mensagem (padrao 1000)")
    a = ap.parse_args()
    servidor() if a.modo == "servidor" else cliente(a.host, a.n, a.tamanho)