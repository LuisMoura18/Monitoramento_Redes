# Protocolo de Aplicação "Sinal" — versão 1.0

Protocolo do chat de mensagens e alertas em tempo real (CIC0124, Projeto 1).
Roda sobre **HTTP/1.1** (operações de consulta, cadastro e configuração) e
**WebSocket** (canal de tempo real), ambos sobre **TCP**, na mesma porta (padrão 8080).

## 1. Visão geral das camadas

| Camada      | O que usamos                                                       |
|-------------|--------------------------------------------------------------------|
| Aplicação   | Protocolo Sinal (mensagens JSON) sobre HTTP/1.1 e WebSocket (RFC 6455) |
| Transporte  | TCP, porta 8080 do servidor; porta efêmera em cada cliente         |
| Rede        | IPv4 (rede local ou loopback)                                      |

### Por que WebSocket para o tempo real

* **O servidor precisa empurrar dados** (mensagens de outros usuários, presença,
  alertas) sem o cliente pedir. Com HTTP puro seria preciso fazer polling, que
  gasta requisições e aumenta o atraso.
* **Funciona direto no navegador**, que não dá acesso a sockets TCP/UDP crus.
* **Roda sobre TCP**: chat e alertas precisam de entrega confiável e em ordem.
  Perder ou inverter uma mensagem de alerta é pior que atrasá-la um pouco,
  então UDP não serve para o canal principal.
* **Uma conexão persistente por cliente**: o handshake TCP e o upgrade HTTP
  acontecem uma única vez, e cada mensagem depois custa só 2 a 14 bytes de
  cabeçalho WebSocket.

HTTP continua sendo usado onde o padrão requisição/resposta é natural: login,
consulta de histórico, alertas, regras e status.

## 2. Formato das mensagens

* Toda mensagem WebSocket é **um quadro de texto (opcode 0x1) contendo um objeto JSON em UTF-8**.
* Todo objeto tem o campo obrigatório **`tipo`** (texto).
* Timestamps (`ts`) são gerados **pelo servidor**, em ISO 8601 UTC com milissegundos
  (ex.: `2026-10-30T19:03:55.733Z`).
* Limites: mensagem WebSocket até 2 MB; campo `conteudo` do chat até 2000 caracteres;
  corpo HTTP até 2 MB.
* Quadros binários não são aceitos (o servidor responde erro `TIPO_NAO_SUPORTADO`).

## 3. Mensagens Cliente → Servidor (WebSocket)

| tipo     | Campos                                                          | Resposta do servidor              |
|----------|-----------------------------------------------------------------|-----------------------------------|
| `msg`    | `conteudo` (texto, 1–2000), `id_cliente` (texto, opcional)      | `ack` para quem enviou + `msg` para toda a sala |
| `entrar` | `sala` (regex `^[a-z0-9_-]{1,20}$`)                             | `sala`                            |
| `ping`   | `seq` (int, opcional), `t` (qualquer, opcional), outros campos livres (padding para testes de throughput) | `pong` |

`id_cliente` é escolhido pelo cliente e volta no `ack` ou no `erro`, para o cliente
saber qual mensagem pendente foi confirmada ou recusada.

## 4. Mensagens Servidor → Cliente (WebSocket)

| tipo        | Campos                                                                  | Quando |
|-------------|-------------------------------------------------------------------------|--------|
| `bem_vindo` | `versao`, `usuario`, `id_conexao`, `sala`, `ts`                          | Logo após o handshake |
| `msg`       | `id`, `sala`, `usuario`, `conteudo`, `ts`                                | Nova mensagem na sala do cliente |
| `ack`       | `id_cliente`, `id` (id no banco), `ts`                                   | Mensagem do próprio cliente foi salva |
| `presenca`  | `online` (lista de usuários), `conexoes` (int), `ts`                     | Alguém conectou ou desconectou |
| `alerta`    | `id`, `regra`, `nivel` (`alto`/`medio`), `usuario`, `sala`, `descricao`, `ts` | Uma regra de alerta disparou (vai para todos) |
| `sala`      | `sala`, `ts`                                                             | Confirmação de troca de sala |
| `pong`      | `seq`, `t` (ecoados), `recebidos_bytes`, `ts`                            | Resposta a `ping` |
| `erro`      | `codigo`, `nome`, `descricao`, `ref` (`id_cliente` original ou null), `ts` | Mensagem inválida |

**Identificação dos clientes:** cada conexão WebSocket recebe um `id_conexao`
(8 caracteres hex) independente do usuário. O mesmo usuário em duas abas ou dois
dispositivos aparece como duas conexões distintas.

## 5. Códigos de erro

Os mesmos códigos são usados no HTTP (status da resposta) e no WebSocket (campo `codigo`).

| codigo | nome                    | Significado |
|--------|-------------------------|-------------|
| 400    | `JSON_INVALIDO`         | Corpo ou quadro não é JSON UTF-8 válido |
| 400    | `TIPO_AUSENTE`          | Falta o campo `tipo` ou ele não é texto |
| 400    | `TIPO_DESCONHECIDO`     | `tipo` não existe no protocolo |
| 400    | `TIPO_NAO_SUPORTADO`    | Quadro WebSocket binário |
| 400    | `HANDSHAKE_INVALIDO`    | Upgrade WebSocket malformado |
| 401    | `NAO_AUTENTICADO`       | Token ausente ou inválido |
| 401    | `CREDENCIAIS_INVALIDAS` | Senha incorreta no login |
| 404    | `NAO_ENCONTRADO`        | Rota ou arquivo inexistente |
| 405    | `METODO_NAO_PERMITIDO`  | Método HTTP não aceito na rota |
| 413    | `MUITO_GRANDE`          | Conteúdo acima do limite |
| 422    | `CAMPO_INVALIDO`        | Campo com formato ou valor inválido |
| 429    | `LIMITE_TAXA`           | Cliente bloqueado temporariamente por flood |
| 500    | `ERRO_INTERNO`          | Falha inesperada no servidor |
| 503    | `SERVIDOR_CHEIO`        | Limite de conexões simultâneas atingido |

Formato do erro em HTTP:
```json
{"erro": {"codigo": 422, "nome": "CAMPO_INVALIDO", "descricao": "sala inválida"}}
```

**Códigos de fechamento WebSocket usados:** 1000 (normal), 1002 (erro de protocolo,
ex.: quadro do cliente sem máscara), 1008 (5 mensagens inválidas seguidas),
1009 (mensagem maior que 2 MB).

## 6. API HTTP

Rotas autenticadas exigem o cabeçalho `Authorization: Bearer <token>`.

| Método | Rota             | Auth | Corpo / parâmetros                                           | Resposta |
|--------|------------------|------|--------------------------------------------------------------|----------|
| POST   | `/api/login`     | não  | `{"usuario", "senha"}`                                       | `201` (usuário criado) ou `200`: `{"token", "usuario", "novo"}` |
| POST   | `/api/logout`    | sim  | —                                                            | `{"ok": true}` |
| GET    | `/api/historico` | sim  | `?sala=geral&limite=50&antes_de=<id>` (limite 1–200)         | `{"sala", "mensagens": [...]}` (mais antiga primeiro) |
| GET    | `/api/alertas`   | sim  | `?limite=50`                                                 | `{"alertas": [...]}` (mais recente primeiro) |
| GET    | `/api/regras`    | sim  | —                                                            | regras atuais |
| PUT    | `/api/regras`    | sim  | qualquer subconjunto de `palavras_chave`, `flood_max_msgs`, `flood_janela_s`, `max_conexoes` | regras atualizadas |
| GET    | `/api/status`    | não  | —                                                            | `{"versao", "online", "conexoes", "uptime_s", "contadores", "ts"}` |
| POST   | `/api/eco`       | sim  | qualquer corpo até 2 MB                                       | `{"recebidos_bytes", "ts"}` (para experimentos de throughput) |
| GET    | `/` e arquivos   | não  | —                                                            | interface web |

## 7. Sequência de operações

```
Cliente                                         Servidor
  |  TCP SYN / SYN-ACK / ACK  (porta 8080)          |
  |------------------------------------------------>|
  |  POST /api/login {"usuario","senha"}            |
  |------------------------------------------------>|  valida, cria sessão
  |<------------------------------------------------|  201 {"token": "..."}
  |  GET /api/historico?sala=geral  (Bearer token)  |
  |------------------------------------------------>|
  |<------------------------------------------------|  200 {"mensagens":[...]}
  |                                                 |
  |  GET /ws?token=...&sala=geral                   |
  |  Upgrade: websocket, Sec-WebSocket-Key          |
  |------------------------------------------------>|  valida token
  |<------------------------------------------------|  101 Switching Protocols
  |<------------------------------------------------|  {"tipo":"bem_vindo",...}
  |<------------------------------------------------|  {"tipo":"presenca",...} (para todos)
  |                                                 |
  |  {"tipo":"msg","id_cliente":"a1","conteudo":..} |
  |------------------------------------------------>|  valida, verifica flood, salva
  |<------------------------------------------------|  {"tipo":"ack","id_cliente":"a1","id":42}
  |<------------------------------------------------|  {"tipo":"msg","id":42,...} (para a sala)
  |<------------------------------------------------|  {"tipo":"alerta",...} (se disparou regra)
  |                                                 |
  |  {"tipo":"ping","seq":7,"t":...}  (a cada 5 s)  |
  |------------------------------------------------>|
  |<------------------------------------------------|  {"tipo":"pong","seq":7,...}
  |<------ quadro WS ping (a cada 20 s) ------------|  detecção de cliente morto
  |------- quadro WS pong (automático) ------------>|
  |                                                 |
  |  quadro WS close (1000)                         |
  |------------------------------------------------>|
  |<------------------------------------------------|  close (1000), FIN TCP
```

## 8. Regras de alerta

| Regra             | Nível | Condição (valores padrão, alteráveis por `PUT /api/regras`) |
|-------------------|-------|--------------------------------------------------------------|
| `palavra_chave`   | alto  | `conteudo` contém um dos termos (urgente, socorro, alerta, falha, caiu, incêndio…) |
| `flood`           | médio | Mais de 8 mensagens em 10 s pela mesma conexão; ela fica bloqueada por 10 s (erro 429) |
| `erros_protocolo` | médio | 5 mensagens inválidas seguidas; a conexão é encerrada (close 1008) |
| `capacidade`      | alto  | Tentativa de conexão com 50 clientes já conectados (503) |

Todo alerta é salvo na tabela `alertas` e enviado a todos os clientes conectados.

## 9. Tratamento de falhas

* **Desconexão do cliente:** o servidor detecta EOF ou erro no socket, remove a
  conexão e avisa os demais com `presenca`.
* **Cliente mudo:** o servidor manda um quadro WS ping a cada 20 s; sem nenhum
  quadro do cliente por 60 s, a conexão é fechada.
* **Cliente lento:** cada conexão tem uma fila de envio de até 500 mensagens.
  Se encher, a conexão é derrubada para não travar o servidor.
* **Servidor indisponível (lado do cliente):** a interface reconecta com espera
  exponencial (1, 2, 4, 8, até 15 s). Antes de reconectar, confere o token via
  HTTP, e depois recarrega o histórico para não perder o que chegou no intervalo.
  Mensagens sem `ack` ficam marcadas como falhas.

## 10. Exemplos

Login:
```http
POST /api/login HTTP/1.1
Content-Type: application/json

{"usuario": "ana", "senha": "1234"}
```
```http
HTTP/1.1 201 Created
Content-Type: application/json; charset=utf-8

{"token": "517a336c8356a34e2ba6a2d9d5c51046", "usuario": "ana", "novo": true}
```

Envio de mensagem que dispara alerta:
```json
→ {"tipo": "msg", "id_cliente": "y1", "conteudo": "servidor CAIU, urgente!"}
← {"tipo": "ack", "id_cliente": "y1", "id": 2, "ts": "2026-10-30T19:03:55.734Z"}
← {"tipo": "msg", "id": 2, "sala": "geral", "usuario": "beto", "conteudo": "servidor CAIU, urgente!", "ts": "2026-10-30T19:03:55.734Z"}
← {"tipo": "alerta", "id": 1, "regra": "palavra_chave", "nivel": "alto", "usuario": "beto", "sala": "geral",
   "descricao": "beto em #geral: \"servidor CAIU, urgente!\" (termos: urgente, caiu)", "ts": "2026-10-30T19:03:55.734Z"}
```

Mensagem inválida:
```json
→ {"tipo": "msg", "id_cliente": "z9", "conteudo": ""}
← {"tipo": "erro", "codigo": 422, "nome": "CAMPO_INVALIDO", "descricao": "'conteudo' deve ser texto não vazio", "ref": "z9", "ts": "..."}
```

Medição de RTT de aplicação:
```json
→ {"tipo": "ping", "seq": 1, "t": 1761851035.12}
← {"tipo": "pong", "seq": 1, "t": 1761851035.12, "recebidos_bytes": 44, "ts": "..."}
```

## 11. Segurança e limitações conhecidas

* Senhas guardadas com PBKDF2-SHA256 (100 mil iterações, sal aleatório por usuário).
* O token vai na URL do WebSocket (`/ws?token=`), porque o navegador não deixa
  mandar cabeçalhos customizados no handshake. Ele não é gravado nos logs.
* **Sem TLS:** em HTTP e WS puros, senha, token e mensagens passam em texto claro
  e aparecem no Wireshark. Em produção seria preciso HTTPS/WSS.
* Tokens não expiram (só com logout).
* Arquitetura centralizada: o servidor é ponto único de falha, e o estado das
  conexões vive na memória de um único processo.
