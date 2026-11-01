"""
Servidor do Chat de Alertas em Tempo Real — CIC0124 Redes de Computadores, Projeto 1.

Um único processo, uma única porta TCP (padrão 8080), atendendo:
  * HTTP/1.1  -> páginas estáticas e API REST (login, histórico, alertas, regras, status, eco)
  * WebSocket -> canal de tempo real (mensagens, presença, alertas, ping/pong de RTT)

Só usa a biblioteca padrão do Python 3.10+ (asyncio, sqlite3, json, hashlib...).
Executar:  python3 server.py [--host 0.0.0.0] [--port 8080]
"""
import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import re
import secrets
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import wsproto as ws

# --------------------------------------------------------------------------- #
# Configuração
# --------------------------------------------------------------------------- #
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
DB_PATH = os.path.join(BASE_DIR, "dados", "chat.db")
LOG_PATH = os.path.join(BASE_DIR, "logs", "servidor.log")

VERSAO_PROTOCOLO = "1.0"
MAX_HTTP_HEADER = 16 * 1024          # bytes
MAX_HTTP_BODY = 2 * 1024 * 1024      # 2 MB (permite experimentos de throughput via HTTP)
MAX_WS_MSG = 2 * 1024 * 1024         # 2 MB (permite experimentos de throughput via WS)
MAX_CONTEUDO = 2000                  # caracteres de uma mensagem de chat
INTERVALO_PING_S = 20                # servidor manda ping WS a cada 20 s
TIMEOUT_INATIVO_S = 60               # sem nenhum quadro do cliente por 60 s -> desconecta
MAX_FILA_ENVIO = 500                 # cliente lento demais é desconectado
MAX_ERROS_PROTOCOLO = 5              # erros seguidos antes de derrubar a conexão

RE_USUARIO = re.compile(r"^[A-Za-z0-9_.-]{3,20}$")
RE_SALA = re.compile(r"^[a-z0-9_-]{1,20}$")

# Regras de alerta (podem ser alteradas em tempo de execução via PUT /api/regras)
REGRAS = {
    "palavras_chave": ["urgente", "socorro", "alerta", "falha", "caiu", "incendio", "incêndio"],
    "flood_max_msgs": 8,      # mais que 8 mensagens...
    "flood_janela_s": 10,     # ...em 10 segundos -> alerta de flood e bloqueio temporário
    "max_conexoes": 50,       # acima disso, novas conexões WS são recusadas
}

INICIO = time.time()
log = logging.getLogger("chat")


def agora_iso() -> str:
    """Timestamp ISO 8601 em UTC com milissegundos."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# --------------------------------------------------------------------------- #
# Persistência (SQLite)
# --------------------------------------------------------------------------- #
class Banco:
    def __init__(self, caminho: str):
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        self.con = sqlite3.connect(caminho, check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.executescript(
            """
            CREATE TABLE IF NOT EXISTS usuarios (
                nome TEXT PRIMARY KEY,
                senha_hash TEXT NOT NULL,
                sal TEXT NOT NULL,
                criado_em TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessoes (
                token TEXT PRIMARY KEY,
                usuario TEXT NOT NULL REFERENCES usuarios(nome),
                ip TEXT,
                criado_em TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mensagens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sala TEXT NOT NULL,
                usuario TEXT NOT NULL,
                conteudo TEXT NOT NULL,
                ts TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_msg_sala ON mensagens(sala, id);
            CREATE TABLE IF NOT EXISTS alertas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                regra TEXT NOT NULL,
                nivel TEXT NOT NULL,
                usuario TEXT,
                sala TEXT,
                descricao TEXT NOT NULL,
                ts TEXT NOT NULL
            );
            """
        )
        self.con.commit()

    @staticmethod
    def _hash(senha: str, sal: str) -> str:
        return hashlib.pbkdf2_hmac("sha256", senha.encode(), bytes.fromhex(sal), 100_000).hex()

    def login(self, nome: str, senha: str, ip: str):
        """Cadastra no primeiro acesso; depois confere a senha. Retorna (token, novo) ou None."""
        row = self.con.execute("SELECT senha_hash, sal FROM usuarios WHERE nome=?", (nome,)).fetchone()
        novo = False
        if row is None:
            sal = secrets.token_hex(16)
            self.con.execute(
                "INSERT INTO usuarios VALUES (?,?,?,?)", (nome, self._hash(senha, sal), sal, agora_iso())
            )
            novo = True
        elif not hmac.compare_digest(row["senha_hash"], self._hash(senha, row["sal"])):
            return None
        token = secrets.token_hex(16)
        self.con.execute("INSERT INTO sessoes VALUES (?,?,?,?)", (token, nome, ip, agora_iso()))
        self.con.commit()
        return token, novo

    def usuario_do_token(self, token: str):
        if not token:
            return None
        row = self.con.execute("SELECT usuario FROM sessoes WHERE token=?", (token,)).fetchone()
        return row["usuario"] if row else None

    def logout(self, token: str):
        self.con.execute("DELETE FROM sessoes WHERE token=?", (token,))
        self.con.commit()

    def salvar_msg(self, sala, usuario, conteudo, ts) -> int:
        cur = self.con.execute(
            "INSERT INTO mensagens (sala, usuario, conteudo, ts) VALUES (?,?,?,?)", (sala, usuario, conteudo, ts)
        )
        self.con.commit()
        return cur.lastrowid

    def historico(self, sala, limite, antes_de=None):
        if antes_de:
            rows = self.con.execute(
                "SELECT * FROM mensagens WHERE sala=? AND id<? ORDER BY id DESC LIMIT ?", (sala, antes_de, limite)
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT * FROM mensagens WHERE sala=? ORDER BY id DESC LIMIT ?", (sala, limite)
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def salvar_alerta(self, regra, nivel, usuario, sala, descricao, ts) -> int:
        cur = self.con.execute(
            "INSERT INTO alertas (regra, nivel, usuario, sala, descricao, ts) VALUES (?,?,?,?,?,?)",
            (regra, nivel, usuario, sala, descricao, ts),
        )
        self.con.commit()
        return cur.lastrowid

    def alertas(self, limite):
        rows = self.con.execute("SELECT * FROM alertas ORDER BY id DESC LIMIT ?", (limite,)).fetchall()
        return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# Estado dos clientes WebSocket
# --------------------------------------------------------------------------- #
class Cliente:
    def __init__(self, usuario, sala, writer, peer):
        self.id = uuid.uuid4().hex[:8]       # identifica a CONEXÃO (um usuário pode ter várias)
        self.usuario = usuario
        self.sala = sala
        self.writer = writer
        self.peer = peer
        self.fila: asyncio.Queue = asyncio.Queue(maxsize=MAX_FILA_ENVIO)
        self.envios_recentes: list[float] = []
        self.bloqueado_ate = 0.0
        self.erros_seguidos = 0
        self.ultimo_quadro = time.monotonic()

    def enviar(self, obj: dict):
        """Enfileira um objeto JSON para envio. Se a fila encher, o cliente é lento demais."""
        dados = ws.encode_frame(json.dumps(obj, ensure_ascii=False).encode())
        try:
            self.fila.put_nowait(dados)
        except asyncio.QueueFull:
            log.warning("fila cheia, desconectando cliente lento %s (%s)", self.id, self.usuario)
            self.writer.close()

    def enviar_cru(self, quadro: bytes):
        try:
            self.fila.put_nowait(quadro)
        except asyncio.QueueFull:
            self.writer.close()


class Servidor:
    def __init__(self, banco: Banco):
        self.banco = banco
        self.clientes: dict[str, Cliente] = {}
        self.contadores = {"http": 0, "ws_msgs": 0, "ws_erros": 0}

    # ------------------------------ util ------------------------------ #
    def online(self):
        return sorted({c.usuario for c in self.clientes.values()})

    def difundir(self, obj: dict, sala: str | None = None):
        for c in list(self.clientes.values()):
            if sala is None or c.sala == sala:
                c.enviar(obj)

    def difundir_presenca(self):
        self.difundir({"tipo": "presenca", "online": self.online(), "conexoes": len(self.clientes), "ts": agora_iso()})

    def gerar_alerta(self, regra, nivel, descricao, usuario=None, sala=None):
        ts = agora_iso()
        aid = self.banco.salvar_alerta(regra, nivel, usuario, sala, descricao, ts)
        log.warning("ALERTA[%s/%s] %s", regra, nivel, descricao)
        self.difundir(
            {"tipo": "alerta", "id": aid, "regra": regra, "nivel": nivel, "usuario": usuario, "sala": sala,
             "descricao": descricao, "ts": ts}
        )

    # ------------------------------ conexão ------------------------------ #
    async def tratar_conexao(self, reader, writer):
        peer = writer.get_extra_info("peername")
        peer_s = f"{peer[0]}:{peer[1]}" if peer else "?"
        log.info("TCP conectado %s", peer_s)
        try:
            while True:
                req = await self.ler_http(reader)
                if req is None:
                    break
                self.contadores["http"] += 1
                if req["headers"].get("upgrade", "").lower() == "websocket":
                    await self.tratar_ws(req, reader, writer, peer_s)
                    break
                manter = await self.tratar_http(req, writer, peer_s)
                if not manter:
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception:
            log.exception("erro inesperado na conexão %s", peer_s)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            log.info("TCP fechado %s", peer_s)

    # ------------------------------ HTTP ------------------------------ #
    async def ler_http(self, reader):
        try:
            cab = await reader.readuntil(b"\r\n\r\n")
        except asyncio.IncompleteReadError as e:
            if e.partial:
                log.info("requisição HTTP incompleta descartada")
            return None
        except asyncio.LimitOverrunError:
            return None
        if len(cab) > MAX_HTTP_HEADER:
            return None
        linhas = cab.decode("iso-8859-1").split("\r\n")
        try:
            metodo, alvo, versao = linhas[0].split(" ", 2)
        except ValueError:
            return None
        headers = {}
        for l in linhas[1:]:
            if ":" in l:
                k, v = l.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        corpo = b""
        tam = int(headers.get("content-length", "0") or 0)
        if tam > MAX_HTTP_BODY:
            return {"metodo": metodo, "erro_tamanho": True, "headers": headers, "caminho": "", "query": {},
                    "corpo": b"", "versao": versao, "inicio": time.perf_counter()}
        if tam:
            corpo = await reader.readexactly(tam)
        url = urlsplit(alvo)
        return {"metodo": metodo.upper(), "caminho": url.path, "query": parse_qs(url.query), "headers": headers,
                "corpo": corpo, "versao": versao, "inicio": time.perf_counter()}

    async def responder(self, writer, status, corpo: bytes, tipo="application/json; charset=utf-8",
                        manter=True, extra=None):
        motivos = {200: "OK", 201: "Created", 204: "No Content", 400: "Bad Request", 401: "Unauthorized",
                   404: "Not Found", 405: "Method Not Allowed", 413: "Payload Too Large",
                   422: "Unprocessable Entity", 429: "Too Many Requests", 500: "Internal Server Error",
                   503: "Service Unavailable"}
        cab = [
            f"HTTP/1.1 {status} {motivos.get(status, '')}",
            f"Content-Type: {tipo}",
            f"Content-Length: {len(corpo)}",
            "Access-Control-Allow-Origin: *",
            "Access-Control-Allow-Headers: Authorization, Content-Type",
            "Access-Control-Allow-Methods: GET, POST, PUT, OPTIONS",
            "Cache-Control: no-store",
            f"Connection: {'keep-alive' if manter else 'close'}",
        ]
        for k, v in (extra or {}).items():
            cab.append(f"{k}: {v}")
        writer.write(("\r\n".join(cab) + "\r\n\r\n").encode() + corpo)
        await writer.drain()

    async def json_ok(self, writer, obj, status=200):
        await self.responder(writer, status, json.dumps(obj, ensure_ascii=False).encode())

    async def json_erro(self, writer, status, nome, descricao, manter=True):
        corpo = json.dumps({"erro": {"codigo": status, "nome": nome, "descricao": descricao}}, ensure_ascii=False)
        await self.responder(writer, status, corpo.encode(), manter=manter)

    def autenticar(self, req):
        auth = req["headers"].get("authorization", "")
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        return self.banco.usuario_do_token(token), token

    async def tratar_http(self, req, writer, peer):
        manter = req["headers"].get("connection", "").lower() != "close"
        if req.get("erro_tamanho"):
            await self.json_erro(writer, 413, "MUITO_GRANDE", f"corpo maior que {MAX_HTTP_BODY} bytes", manter=False)
            return False
        m, p = req["metodo"], req["caminho"]
        status = 200
        try:
            if m == "OPTIONS":
                await self.responder(writer, 204, b"")
            elif p.startswith("/api/"):
                status = await self.rota_api(m, p, req, writer, peer)
            elif m == "GET":
                status = await self.servir_estatico(p, writer)
            else:
                status = 405
                await self.json_erro(writer, 405, "METODO_NAO_PERMITIDO", f"{m} não é aceito em {p}")
        except json.JSONDecodeError:
            status = 400
            await self.json_erro(writer, 400, "JSON_INVALIDO", "o corpo não é um JSON válido")
        except Exception:
            log.exception("erro tratando %s %s", m, p)
            status = 500
            await self.json_erro(writer, 500, "ERRO_INTERNO", "erro inesperado no servidor")
        dur_ms = (time.perf_counter() - req["inicio"]) * 1000
        log.info("HTTP %s %s %s -> %d (%.2f ms)", peer, m, p, status, dur_ms)
        return manter

    async def servir_estatico(self, caminho, writer):
        if caminho in ("", "/"):
            caminho = "/index.html"
        alvo = os.path.normpath(os.path.join(STATIC_DIR, caminho.lstrip("/")))
        if not alvo.startswith(STATIC_DIR + os.sep) or not os.path.isfile(alvo):
            await self.json_erro(writer, 404, "NAO_ENCONTRADO", "recurso inexistente")
            return 404
        tipo = mimetypes.guess_type(alvo)[0] or "application/octet-stream"
        if tipo.startswith("text/") or tipo.endswith("javascript"):
            tipo += "; charset=utf-8"
        with open(alvo, "rb") as f:
            await self.responder(writer, 200, f.read(), tipo)
        return 200

    async def rota_api(self, m, p, req, writer, peer):
        q = req["query"]

        # ---- públicas ----
        if p == "/api/status" and m == "GET":
            await self.json_ok(writer, {
                "versao": VERSAO_PROTOCOLO, "online": self.online(), "conexoes": len(self.clientes),
                "uptime_s": round(time.time() - INICIO, 1), "contadores": self.contadores, "ts": agora_iso()})
            return 200

        if p == "/api/login" and m == "POST":
            dados = json.loads(req["corpo"] or b"{}")
            nome = str(dados.get("usuario", "")).strip()
            senha = str(dados.get("senha", ""))
            if not RE_USUARIO.match(nome):
                await self.json_erro(writer, 422, "CAMPO_INVALIDO",
                                     "usuario deve ter 3-20 caracteres: letras, números, _ . -")
                return 422
            if not 4 <= len(senha) <= 64:
                await self.json_erro(writer, 422, "CAMPO_INVALIDO", "senha deve ter de 4 a 64 caracteres")
                return 422
            r = self.banco.login(nome, senha, peer)
            if r is None:
                log.warning("login recusado para %s de %s", nome, peer)
                await self.json_erro(writer, 401, "CREDENCIAIS_INVALIDAS", "senha incorreta")
                return 401
            token, novo = r
            log.info("login ok %s (novo=%s) de %s", nome, novo, peer)
            await self.json_ok(writer, {"token": token, "usuario": nome, "novo": novo}, 201 if novo else 200)
            return 201 if novo else 200

        # ---- autenticadas ----
        usuario, token = self.autenticar(req)
        if not usuario:
            await self.json_erro(writer, 401, "NAO_AUTENTICADO", "envie Authorization: Bearer <token>")
            return 401

        if p == "/api/logout" and m == "POST":
            self.banco.logout(token)
            await self.json_ok(writer, {"ok": True})
            return 200

        if p == "/api/historico" and m == "GET":
            sala = q.get("sala", ["geral"])[0]
            if not RE_SALA.match(sala):
                await self.json_erro(writer, 422, "CAMPO_INVALIDO", "sala inválida")
                return 422
            try:
                limite = max(1, min(200, int(q.get("limite", ["50"])[0])))
                antes = int(q["antes_de"][0]) if "antes_de" in q else None
            except ValueError:
                await self.json_erro(writer, 422, "CAMPO_INVALIDO", "limite/antes_de devem ser inteiros")
                return 422
            await self.json_ok(writer, {"sala": sala, "mensagens": self.banco.historico(sala, limite, antes)})
            return 200

        if p == "/api/alertas" and m == "GET":
            try:
                limite = max(1, min(200, int(q.get("limite", ["50"])[0])))
            except ValueError:
                limite = 50
            await self.json_ok(writer, {"alertas": self.banco.alertas(limite)})
            return 200

        if p == "/api/regras":
            if m == "GET":
                await self.json_ok(writer, REGRAS)
                return 200
            if m == "PUT":
                dados = json.loads(req["corpo"] or b"{}")
                erro = atualizar_regras(dados)
                if erro:
                    await self.json_erro(writer, 422, "CAMPO_INVALIDO", erro)
                    return 422
                log.info("regras alteradas por %s: %s", usuario, dados)
                await self.json_ok(writer, REGRAS)
                return 200

        if p == "/api/eco" and m == "POST":
            # Para experimentos de throughput via HTTP: devolve só o tamanho recebido.
            await self.json_ok(writer, {"recebidos_bytes": len(req["corpo"]), "ts": agora_iso()})
            return 200

        await self.json_erro(writer, 404, "NAO_ENCONTRADO", f"rota {m} {p} inexistente")
        return 404

    # ------------------------------ WebSocket ------------------------------ #
    async def tratar_ws(self, req, reader, writer, peer):
        h = req["headers"]
        token = req["query"].get("token", [""])[0]
        usuario = self.banco.usuario_do_token(token)
        sala = req["query"].get("sala", ["geral"])[0]
        chave = h.get("sec-websocket-key")

        if req["caminho"] != "/ws" or not chave or h.get("sec-websocket-version") != "13":
            await self.json_erro(writer, 400, "HANDSHAKE_INVALIDO", "use GET /ws com cabeçalhos WebSocket", False)
            return
        if not usuario:
            log.warning("WS recusado (token inválido) de %s", peer)
            await self.json_erro(writer, 401, "NAO_AUTENTICADO", "token ausente ou inválido", False)
            return
        if not RE_SALA.match(sala):
            await self.json_erro(writer, 422, "CAMPO_INVALIDO", "sala inválida", False)
            return
        if len(self.clientes) >= REGRAS["max_conexoes"]:
            self.gerar_alerta("capacidade", "alto", f"limite de {REGRAS['max_conexoes']} conexões atingido")
            await self.json_erro(writer, 503, "SERVIDOR_CHEIO", "tente novamente mais tarde", False)
            return

        writer.write(
            ("HTTP/1.1 101 Switching Protocols\r\n"
             "Upgrade: websocket\r\nConnection: Upgrade\r\n"
             f"Sec-WebSocket-Accept: {ws.accept_key(chave)}\r\n\r\n").encode()
        )
        await writer.drain()

        cli = Cliente(usuario, sala, writer, peer)
        self.clientes[cli.id] = cli
        log.info("WS aberto %s usuario=%s conexao=%s sala=%s", peer, usuario, cli.id, sala)
        cli.enviar({"tipo": "bem_vindo", "versao": VERSAO_PROTOCOLO, "usuario": usuario, "id_conexao": cli.id,
                    "sala": sala, "ts": agora_iso()})
        self.difundir_presenca()

        tarefa_envio = asyncio.create_task(self.laco_envio(cli))
        tarefa_ping = asyncio.create_task(self.laco_ping(cli))
        motivo = "normal"
        try:
            motivo = await self.laco_leitura(cli, reader)
        except asyncio.IncompleteReadError:
            motivo = "conexão interrompida pelo cliente"
        except ConnectionError as e:
            motivo = f"erro de conexão: {e.__class__.__name__}"
        except ws.FrameTooLarge:
            motivo = "mensagem grande demais"
            cli.enviar_cru(ws.encode_close(1009, "mensagem grande demais"))
        except ws.ProtocolError as e:
            motivo = f"erro de protocolo WS: {e}"
            cli.enviar_cru(ws.encode_close(1002, "erro de protocolo"))
        finally:
            self.clientes.pop(cli.id, None)
            tarefa_ping.cancel()
            await asyncio.sleep(0.05)  # deixa sair o quadro de close, se houver
            tarefa_envio.cancel()
            log.info("WS fechado %s usuario=%s conexao=%s motivo=%s", peer, usuario, cli.id, motivo)
            self.difundir_presenca()

    async def laco_envio(self, cli: Cliente):
        try:
            while True:
                quadro = await cli.fila.get()
                cli.writer.write(quadro)
                await cli.writer.drain()
        except (asyncio.CancelledError, ConnectionError):
            pass

    async def laco_ping(self, cli: Cliente):
        """Detecta clientes mortos: manda ping WS periódico e derruba quem ficou mudo."""
        try:
            while True:
                await asyncio.sleep(INTERVALO_PING_S)
                if time.monotonic() - cli.ultimo_quadro > TIMEOUT_INATIVO_S:
                    log.warning("cliente %s (%s) inativo há mais de %ds, desconectando",
                                cli.id, cli.usuario, TIMEOUT_INATIVO_S)
                    cli.writer.close()
                    return
                cli.enviar_cru(ws.encode_frame(str(time.time()).encode(), ws.OP_PING))
        except asyncio.CancelledError:
            pass

    async def laco_leitura(self, cli: Cliente, reader) -> str:
        while True:
            op, dados, mascarado = await ws.read_message(reader, MAX_WS_MSG)
            cli.ultimo_quadro = time.monotonic()
            if not mascarado:
                cli.enviar_cru(ws.encode_close(1002, "quadros do cliente devem ser mascarados"))
                return "quadro sem máscara"
            if op == ws.OP_CLOSE:
                cli.enviar_cru(ws.encode_close(1000))
                return "close enviado pelo cliente"
            if op == ws.OP_PING:
                cli.enviar_cru(ws.encode_frame(dados, ws.OP_PONG))
                continue
            if op == ws.OP_PONG:
                continue
            if op == ws.OP_BIN:
                self.erro_ws(cli, 400, "TIPO_NAO_SUPORTADO", "use quadros de texto com JSON")
            else:
                self.processar(cli, dados)
            if cli.erros_seguidos >= MAX_ERROS_PROTOCOLO:
                self.gerar_alerta("erros_protocolo", "medio",
                                  f"conexão {cli.id} de {cli.usuario} enviou {cli.erros_seguidos} mensagens inválidas seguidas e foi desconectada",
                                  cli.usuario, cli.sala)
                cli.enviar_cru(ws.encode_close(1008, "mensagens inválidas demais"))
                return "excesso de mensagens inválidas"

    def erro_ws(self, cli, codigo, nome, descricao, ref=None):
        cli.erros_seguidos += 1
        self.contadores["ws_erros"] += 1
        log.info("WS erro -> %s (%s): %s %s", cli.id, cli.usuario, nome, descricao)
        cli.enviar({"tipo": "erro", "codigo": codigo, "nome": nome, "descricao": descricao, "ref": ref,
                    "ts": agora_iso()})

    def processar(self, cli: Cliente, bruto: bytes):
        try:
            msg = json.loads(bruto.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return self.erro_ws(cli, 400, "JSON_INVALIDO", "a mensagem não é um JSON UTF-8 válido")
        if not isinstance(msg, dict) or not isinstance(msg.get("tipo"), str):
            return self.erro_ws(cli, 400, "TIPO_AUSENTE", "toda mensagem precisa do campo 'tipo' (texto)")

        tipo = msg["tipo"]
        ref = msg.get("id_cliente")
        self.contadores["ws_msgs"] += 1

        if tipo == "ping":
            # RTT de aplicação: o cliente manda t (seu relógio) e mede quando o pong volta.
            cli.erros_seguidos = 0
            cli.enviar({"tipo": "pong", "t": msg.get("t"), "seq": msg.get("seq"),
                        "recebidos_bytes": len(bruto), "ts": agora_iso()})
            return

        if tipo == "entrar":
            sala = msg.get("sala")
            if not isinstance(sala, str) or not RE_SALA.match(sala):
                return self.erro_ws(cli, 422, "CAMPO_INVALIDO", "sala inválida (a-z, 0-9, _ -, até 20)", ref)
            cli.sala = sala
            cli.erros_seguidos = 0
            log.info("conexao %s (%s) entrou na sala %s", cli.id, cli.usuario, sala)
            cli.enviar({"tipo": "sala", "sala": sala, "ts": agora_iso()})
            return

        if tipo == "msg":
            conteudo = msg.get("conteudo")
            if not isinstance(conteudo, str) or not conteudo.strip():
                return self.erro_ws(cli, 422, "CAMPO_INVALIDO", "'conteudo' deve ser texto não vazio", ref)
            conteudo = conteudo.strip()
            if len(conteudo) > MAX_CONTEUDO:
                return self.erro_ws(cli, 413, "MUITO_GRANDE", f"máximo de {MAX_CONTEUDO} caracteres", ref)

            # Regra de flood: muitas mensagens em pouco tempo
            agora = time.monotonic()
            if agora < cli.bloqueado_ate:
                return self.erro_ws(cli, 429, "LIMITE_TAXA",
                                    f"aguarde {cli.bloqueado_ate - agora:.0f}s para enviar de novo", ref)
            janela = REGRAS["flood_janela_s"]
            cli.envios_recentes = [t for t in cli.envios_recentes if agora - t < janela] + [agora]
            if len(cli.envios_recentes) > REGRAS["flood_max_msgs"]:
                cli.bloqueado_ate = agora + janela
                cli.envios_recentes.clear()
                self.gerar_alerta("flood", "medio",
                                  f"{cli.usuario} enviou mais de {REGRAS['flood_max_msgs']} mensagens em {janela}s",
                                  cli.usuario, cli.sala)
                return self.erro_ws(cli, 429, "LIMITE_TAXA", f"bloqueado por {janela}s (flood)", ref)

            cli.erros_seguidos = 0
            ts = agora_iso()
            mid = self.banco.salvar_msg(cli.sala, cli.usuario, conteudo, ts)
            cli.enviar({"tipo": "ack", "id_cliente": ref, "id": mid, "ts": ts})
            self.difundir({"tipo": "msg", "id": mid, "sala": cli.sala, "usuario": cli.usuario,
                           "conteudo": conteudo, "ts": ts}, sala=cli.sala)

            # Regra de palavra-chave
            baixo = conteudo.lower()
            achadas = [p for p in REGRAS["palavras_chave"] if p.lower() in baixo]
            if achadas:
                self.gerar_alerta("palavra_chave", "alto",
                                  f"{cli.usuario} em #{cli.sala}: \"{conteudo[:80]}\" (termos: {', '.join(achadas)})",
                                  cli.usuario, cli.sala)
            return

        return self.erro_ws(cli, 400, "TIPO_DESCONHECIDO", f"tipo '{tipo[:30]}' não existe no protocolo", ref)


def atualizar_regras(dados) -> str | None:
    """Valida e aplica alterações nas regras. Retorna mensagem de erro ou None."""
    if not isinstance(dados, dict):
        return "corpo deve ser um objeto JSON"
    novas = {}
    if "palavras_chave" in dados:
        pc = dados["palavras_chave"]
        if not isinstance(pc, list) or not all(isinstance(x, str) and 1 <= len(x) <= 30 for x in pc) or len(pc) > 50:
            return "palavras_chave deve ser lista de até 50 textos (1-30 caracteres)"
        novas["palavras_chave"] = [x.strip() for x in pc]
    for campo, lo, hi in (("flood_max_msgs", 1, 1000), ("flood_janela_s", 1, 3600), ("max_conexoes", 1, 10000)):
        if campo in dados:
            v = dados[campo]
            if not isinstance(v, int) or isinstance(v, bool) or not lo <= v <= hi:
                return f"{campo} deve ser inteiro entre {lo} e {hi}"
            novas[campo] = v
    desconhecidos = set(dados) - set(REGRAS)
    if desconhecidos:
        return f"campos desconhecidos: {', '.join(sorted(desconhecidos))}"
    REGRAS.update(novas)
    return None


def configurar_logs():
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.INFO)
    for h in (logging.StreamHandler(), logging.FileHandler(LOG_PATH, encoding="utf-8")):
        h.setFormatter(fmt)
        log.addHandler(h)


async def main():
    ap = argparse.ArgumentParser(description="Servidor do Chat de Alertas (HTTP + WebSocket)")
    ap.add_argument("--host", default="0.0.0.0", help="interface (0.0.0.0 = aceita conexões da rede)")
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()

    configurar_logs()
    srv = Servidor(Banco(DB_PATH))
    server = await asyncio.start_server(srv.tratar_conexao, args.host, args.port, limit=MAX_HTTP_HEADER)
    log.info("servidor no ar em http://%s:%d  (protocolo v%s)", args.host, args.port, VERSAO_PROTOCOLO)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nservidor encerrado")
