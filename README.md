# Sinal — Chat de Mensagens e Alertas em Tempo Real

Projeto 1 de CIC0124 Redes de Computadores (UnB, 2026).
Aplicação distribuída com interface web, servidor HTTP + WebSocket, persistência em
SQLite e alertas automáticos. O protocolo de aplicação está em [PROTOCOLO.md](PROTOCOLO.md).

## Requisitos

* **Python 3.10 ou mais novo.** Nenhuma biblioteca externa: tudo usa a biblioteca padrão
  (o WebSocket foi implementado do zero em `wsproto.py`, seguindo a RFC 6455).
* Um navegador moderno (Chrome, Firefox, Edge, Safari) para a interface.

## Como executar

```bash
python3 server.py                 # escuta em 0.0.0.0:8080
python3 server.py --port 9000     # outra porta
```

Abra **http://localhost:8080** no navegador. No primeiro login, o usuário é criado
com a senha digitada; depois a senha passa a ser conferida.

### Acessando de outro computador ou do celular (mesma rede)

1. Descubra o IP da máquina do servidor: `ip addr` (Linux), `ipconfig` (Windows)
   ou `ifconfig` (macOS). Exemplo: `192.168.0.10`.
2. No outro dispositivo, abra `http://192.168.0.10:8080`.
3. Se não abrir, libere a porta 8080 no firewall da máquina do servidor.

### Testando com vários clientes

* Abra duas abas ou janelas anônimas e entre com usuários diferentes.
* Ou use o cliente de linha de comando:

```bash
python3 cliente_teste.py --usuario bot1 --senha 1234 chat
python3 cliente_teste.py --usuario bot2 --senha 1234 rtt -n 30
python3 cliente_teste.py --usuario bot3 --senha 1234 enviar "teste urgente" -n 3
python3 cliente_teste.py --host 192.168.0.10 --usuario bot4 --senha 1234 rtt -n 30
```

### Testando a API HTTP com curl

```bash
curl http://localhost:8080/api/status
TOKEN=$(curl -s -X POST localhost:8080/api/login -d '{"usuario":"ana","senha":"1234"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
curl -H "Authorization: Bearer $TOKEN" "localhost:8080/api/historico?sala=geral&limite=10"
curl -H "Authorization: Bearer $TOKEN" localhost:8080/api/alertas
curl -X PUT -H "Authorization: Bearer $TOKEN" localhost:8080/api/regras -d '{"flood_max_msgs": 5}'
```

## Estrutura

```
server.py          servidor: HTTP, WebSocket, SQLite, regras de alerta, logs
wsproto.py         implementação do WebSocket (handshake, quadros, máscara, ping/pong, close)
cliente_teste.py   cliente de terminal (chat, medição de RTT, envio em lote)
static/index.html  interface web (HTML + CSS + JavaScript puro)
PROTOCOLO.md       especificação do protocolo de aplicação
dados/chat.db      banco SQLite (criado na primeira execução)
logs/servidor.log  log com timestamps em milissegundos (criado na primeira execução)
```

Para começar com banco e logs zerados, apague as pastas `dados/` e `logs/`.

## O que a aplicação faz

* **Login** via HTTP (`POST /api/login`), com senha guardada como PBKDF2 e token de sessão.
* **Tempo real** via WebSocket: mensagens, lista de quem está online e alertas.
* **Salas** (#geral, #laboratorio, #infra).
* **Histórico** persistente em SQLite, consultado via HTTP com paginação.
* **Alertas automáticos:** palavra-chave (urgente, caiu, falha…), flood (mais de 8
  mensagens em 10 s), mensagens inválidas repetidas e servidor cheio. Os limites
  podem ser alterados por `PUT /api/regras`.
* **Robustez:** validação de todos os campos, erros padronizados, reconexão automática
  com espera exponencial, ping periódico para detectar clientes mortos e
  desconexão de clientes lentos.
* **Medição na interface:** o RTT de aplicação (ping/pong a cada 5 s) aparece no topo,
  e passar o mouse sobre uma mensagem enviada mostra quanto tempo levou o `ack`.

## Dicas para os experimentos e o Wireshark

* Filtro de captura e exibição: `tcp.port == 8080`.
* Para ver o tráfego da mesma máquina, capture na interface **loopback** (`lo` / `Loopback`).
* `http` mostra o login e o upgrade (`101 Switching Protocols`); `websocket` mostra os quadros.
  O Wireshark decodifica o JSON porque não há TLS.
* `GET /api/status` (sem autenticação) é bom para medir tempo de resposta HTTP.
* `POST /api/eco` e o tipo `ping` com campos extras grandes (até 2 MB) servem para throughput.
* O tempo de cada requisição HTTP já sai no log do servidor (`... -> 200 (0.12 ms)`).

## Ferramentas e uso de IA

*O uso de IA foi destinado para configuração do Wireshark, para conferir os scripts de experimentos e para realizar a frontend e ajustes e correções de códigos da backend do site.*
