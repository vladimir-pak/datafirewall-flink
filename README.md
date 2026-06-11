# DataFirewall Flink

**DataFirewall Flink** — потоковая Apache Flink Job для обработки request-response сообщений из IBM MQ или Apache ActiveMQ Artemis, применения бизнес-проверок, формирования ответа и отправки результата обратно в MQ.

Job поддерживает два режима обработки:

- **`handler=flink`** — сообщение обрабатывается внутри Flink через `MessageProcessingService`.
- **`handler=dotnet`** — сообщение синхронно отправляется во внешний .NET-сервис по REST API, а исходный запрос дополнительно отправляется в Kafka для shadow processing отдельной Flink Job (опционально).

---

## Содержание

- [Основные возможности](#основные-возможности)
- [Технологии](#технологии)
- [Архитектура](#архитектура)
- [Режимы обработки](#режимы-обработки)
- [Обновление правил](#обновление-правил)
- [Интеграции](#интеграции)
- [Vault secrets](#vault-secrets)
- [Конфигурация](#конфигурация)
- [Примеры запуска](#примеры-запуска)
- [Рекомендации по parallelism](#рекомендации-по-parallelism)
- [Мониторинг и аудит](#мониторинг-и-аудит)
- [Диагностика](#диагностика)

---

## Основные возможности

- Чтение сообщений из **IBM MQ** или **Apache ActiveMQ Artemis**.
- Отправка ответа обратно в MQ/Artemis в режиме request-response.
- Горизонтальное масштабирование через `parallelism.source`, `parallelism.process`, `parallelism.sink`.
- Динамическое обновление правил через Kafka broadcast stream.
- Bootstrap правил и справочников через Ignite API.
- Runtime-переключение обработчика `flink` / `dotnet` через Kafka-событие.
- Shadow processing для режима `handler=dotnet` через отдельный Kafka topic.
- Kafka-аудит детального результата обработки.
- CEF-аудит системных событий job, Kafka, MQ и изменения конфигурации.
- Получение секретов из Vault.

---

## Технологии

- Java 17
- Apache Flink 1.20.x
- Apache Kafka
- IBM MQ
- Apache ActiveMQ Artemis
- Apache Ignite API
- HashiCorp Vault
- Maven

---

## Архитектура

### Общая схема

```text
                    +-----------------------+
                    | Kafka rules-update    |
                    | cache / handler event |
                    +-----------+-----------+
                                |
                                v
+---------+        +------------+-------------+        +-------------+
| IBM MQ  | -----> | Flink Processing Job    | -----> | IBM MQ      |
| Artemis |        |                         |        | Artemis     |
+---------+        +------------+-------------+        +-------------+
                                |
                                +----> Kafka audit/detail-answer
                                |
                                +----> Kafka shadow-processing
                                      только при handler=dotnet
```

Job состоит из нескольких независимых стадий:

| Стадия | Назначение |
|---|---|
| `mq-source` / `artemis-source` | Читает входящие сообщения из MQ/Artemis. |
| `rules-kafka-source` | Читает события обновления правил и handler routing из Kafka. |
| `process-with-rules-reload` | Обрабатывает сообщения, применяет актуальные правила или вызывает .NET handler. |
| `mq-sink` / `artemis-sink` | Отправляет short answer обратно в MQ/Artemis. |
| `audit-kafka-sink` | Отправляет audit/detail record в Kafka. |
| `dotnet-shadow-kafka-sink` | Отправляет исходный запрос в Kafka для shadow processing при `handler=dotnet`. |

---

## Режимы обработки

### `handler=flink`

В этом режиме Flink сам выполняет основную бизнес-обработку.

```text
MQ/Artemis request
  -> MqSource / ArtemisSource
  -> MessageProcessingService
  -> ProcessingResult.shortJson
  -> MqSink / ArtemisSink
```

`MessageProcessingService` формирует:

- `originalJson` — исходный запрос;
- `shortJson` — краткий ответ для MQ/Artemis;
- `detailJson` — детальный ответ для Kafka-аудита;
- correlation id для корректного request-response ответа.

### `handler=dotnet`

В этом режиме основным обработчиком является внешний .NET-сервис.

```text
MQ/Artemis request
  -> MqSource / ArtemisSource
  -> REST call в .NET handler
  -> ответ .NET
  -> ProcessingResult
  -> MqSink / ArtemisSink
```

Параллельно исходный MQ/Artemis request отправляется в Kafka topic для дальнейшего shadow processing отдельной Flink Job:

```text
MQ/Artemis request
  -> dotnet-shadow-kafka-sink
  -> Kafka topic datafirewall.shadow-processing
  -> отдельная shadow Flink Job
```

Ответ .NET-сервиса должен иметь структуру:

```json
{
  "answer": {},
  "detailAnswer": {}
}
```

Маппинг ответа:

| Поле .NET response | Поле `ProcessingResult` |
|---|---|
| `answer` | `shortJson` |
| `detailAnswer` | `detailJson` |
| исходный MQ/Artemis request | `originalJson` |

Для REST API используется Bearer authorization:

```http
Authorization: Bearer <dotnetJwt>
```

`dotnetJwt` берется из Vault и является обязательным только при `handler=dotnet`.

---

## Обновление правил

Правила обновляются через Kafka topic `rules-update`.

Kafka source работает с `parallelism=1`, после чего событие распространяется всем processing subtasks через broadcast stream.

Пример события обновления версии правил:

```json
{
  "cacheName": "compiled_rules",
  "version": 4
}
```

Пример события переключения handler:

```json
{
  "cacheName": "handler",
  "handler": "dotnet"
}
```

При получении события каждый processing subtask:

1. Проверяет версию события.
2. Загружает актуальный cache через Ignite API.
3. Компилирует правила, если применимо.
4. Атомарно переключает локальный runtime snapshot.
5. Продолжает обработку уже с новой версией.

Правила хранятся локально внутри subtask. После успешной загрузки и компиляции ссылка на runtime snapshot заменяется целиком. Это позволяет избежать промежуточных неконсистентных состояний.

---

## Интеграции

### IBM MQ

Используется для request-response сценария.

- `MqSource` читает сообщения из входной очереди.
- `MqSink` отправляет краткий ответ в выходную очередь.
- Поддерживается TLS.
- Поддерживаются MQ correlation id и дополнительные MQ properties.

Основные MQ headers/properties для ответа:

| Поле | Описание |
|---|---|
| `correlationId` | Берется из исходного MQ message id. |
| `X_From` | Значение из конфигурации, fallback `MKD`. |
| `X_ServiceID` | Значение из конфигурации. |
| `X_CreateDateTime` | UTC-время отправки ответа с точностью до миллисекунд. |

### Apache ActiveMQ Artemis

Альтернативный backend для request-response обработки.

- `ArtemisSource` читает входящие сообщения.
- `ArtemisSink` отправляет ответы.
- Поддерживается TLS через truststore/keystore.

### Kafka

Kafka используется для нескольких независимых потоков:

| Назначение | Параметры |
|---|---|
| Обновление правил | `kafka.*` |
| Audit/detail answer | `audit.kafka.*` |
| CEF-аудит | `cef.audit.kafka.*` |
| Shadow processing для .NET handler | `handler.dotnet.shadow.kafka.*` |

Если специализированные Kafka security properties не заданы, используется fallback:

```text
handler.dotnet.shadow.kafka.* -> kafka.*
audit.kafka.*                 -> kafka.*
cef.audit.kafka.*              -> audit.kafka.* -> kafka.*
```

### Ignite API

Используется для загрузки правил и справочников:

- compiled rules;
- politics datasets;
- служебные cache snapshots.

Поддерживается HTTP/HTTPS и CA certificate через PEM.

### Vault

Vault используется для получения секретов:

- MQ/Artemis credentials;
- Kafka credentials;
- truststore/keystore passwords;
- JWT для Ignite API;
- JWT для .NET API.

---

## Vault secrets

Ожидаемые поля Vault secret:

| Поле | Обязательность | Назначение |
|---|---:|---|
| `mqUser` | для `mq`/`artemis` | Пользователь MQ/Artemis. |
| `mqPassword` | для `mq`/`artemis` | Пароль MQ/Artemis. |
| `mqTruststorePassword` | если включен MQ/Artemis TLS | Пароль truststore. |
| `mqKeystorePassword` | если включен Artemis mutual TLS | Пароль keystore. |
| `kafkaUser` | если включен Kafka SASL | Пользователь Kafka. |
| `kafkaPassword` | если включен Kafka SASL | Пароль Kafka. |
| `truststorePassword` | если включен Kafka TLS | Пароль Kafka truststore. |
| `keystorePassword` | если включен Kafka TLS | Пароль Kafka keystore. |
| `jwt` | если Ignite API требует Bearer token | JWT для Ignite API. |
| `dotnetJwt` | только при `handler=dotnet` | JWT для .NET handler API. |

Пример структуры Vault secret:

```json
{
  "mqUser": "app-user",
  "mqPassword": "***",
  "mqTruststorePassword": "***",
  "mqKeystorePassword": "***",
  "kafkaUser": "kafka-user",
  "kafkaPassword": "***",
  "truststorePassword": "***",
  "keystorePassword": "***",
  "jwt": "***",
  "dotnetJwt": "***"
}
```

---

## Конфигурация

Параметры можно передавать через CLI:

```bash
--messaging.backend=mq --handler=flink
```

или через properties-файл:

```bash
--config.file=/opt/flink/conf/datafirewall-flink.properties
```

CLI-параметры имеют приоритет над properties-файлом.

### Базовые параметры

```properties
# Backend входящих/исходящих сообщений
messaging.backend=mq
# значения: mq | artemis

# Основной обработчик
handler=flink
# значения: flink | dotnet

# Значение по умолчанию для runtime handler state
handler.default=flink
```

### Parallelism

```properties
parallelism=4

# Необязательная детализация по стадиям
parallelism.source=4
parallelism.process=4
parallelism.sink=4
parallelism.shadow.kafka=2
parallelism.audit=2
```

### Checkpointing

```properties
flink.checkpoint.enabled=true
flink.checkpoint.interval.ms=5000
flink.checkpoint.timeout.ms=60000
flink.checkpoint.min.pause.ms=1000
flink.checkpoint.max.concurrent=1
```

### Rules / cache

```properties
rules.loader=http
ignite.apiUrl=http://127.0.0.1:8080
ignite.api.tls.ca.pem=/opt/flink/certs/ca.pem

cache.bootstrap.enabled=true
politics.bootstrap.enabled=false

test.politic.caches.enabled=false
test.politic.caches.path=/opt/flink/conf/test_politic_caches.json
```

### Logging

```properties
log.payloads=false
log.preview.len=600
```

---

## Kafka configuration

### Kafka для rules update

```properties
kafka.bootstrap=localhost:9092
kafka.topic=rules-update
kafka.group=dfw-rules-group

# TLS/SASL, если требуется
kafka.tls.enabled=true
kafka.security.protocol=SASL_SSL
kafka.sasl.mechanism=SCRAM-SHA-512

kafka.ssl.truststore.location=/opt/flink/certs/truststore.p12
kafka.ssl.truststore.type=PKCS12
kafka.ssl.keystore.location=/opt/flink/certs/keystore.p12
kafka.ssl.keystore.type=PKCS12
kafka.ssl.endpoint.identification.algorithm=https
```

Kafka credentials и пароли truststore/keystore берутся из Vault.

### Kafka audit/detail answer

```properties
audit.kafka.enabled=true
audit.kafka.bootstrap=localhost:9092
audit.kafka.topic=detail-answer

# Если audit.kafka.* не задан, параметры подключения наследуются из kafka.*
audit.kafka.tls.enabled=true
audit.kafka.security.protocol=SASL_SSL
audit.kafka.sasl.mechanism=SCRAM-SHA-512
audit.kafka.ssl.truststore.location=/opt/flink/certs/truststore.p12
audit.kafka.ssl.truststore.type=PKCS12
audit.kafka.ssl.keystore.location=/opt/flink/certs/keystore.p12
audit.kafka.ssl.keystore.type=PKCS12
audit.kafka.ssl.endpoint.identification.algorithm=https
```

### Kafka shadow processing для `handler=dotnet`

```properties
handler.dotnet.shadow.kafka.enabled=true
handler.dotnet.shadow.kafka.topic=datafirewall.shadow-processing
handler.dotnet.shadow.kafka.bootstrap=kafka-host:9092
```

Назначение topic — сохранить исходный MQ/Artemis request для дальнейшей асинхронной обработки отдельной Flink Job.

---

## IBM MQ configuration

```properties
mq.host=localhost
mq.port=1414
mq.channel=DEV.APP.SVRCONN
mq.qmgr=QM1

mq.inQueue=IN.Q
mq.outQueue=OUT.Q
mq.wait.interval.ms=1000

# TLS
mq.tls.enabled=false
mq.tls.cipherSuite=
mq.ssl.truststore.location=/opt/flink/certs/truststore.p12
mq.ssl.truststore.type=PKCS12
```

Дополнительные headers/properties ответа:

```properties
mq.reply.xFrom=MKD
mq.reply.xServiceId=datafirewall
```

---

## Artemis configuration

```properties
artemis.broker.url=tcp://localhost:61616
artemis.in.queue=IN.Q
artemis.out.queue=OUT.Q
artemis.receive.timeout.ms=1000

# TLS
artemis.tls.enabled=false
artemis.ssl.truststore.location=/opt/flink/certs/truststore.p12
artemis.ssl.keystore.location=/opt/flink/certs/keystore.p12
artemis.ssl.enabled.cipher.suites=
```

---

## .NET handler configuration

Используется только при:

```properties
handler=dotnet
```

```properties
handler.dotnet.url=http://dotnet-service:8080/api/v1/datafirewall/process
handler.dotnet.timeout.ms=20000
```

Для авторизации используется JWT из Vault:

```text
dotnetJwt
```

HTTP request отправляется с заголовком:

```http
Authorization: Bearer <dotnetJwt>
```

Ожидаемый HTTP response:

```json
{
  "answer": {
    "ALL_RESULT": "SUCCESS"
  },
  "detail_answer": {
    "ALL_RESULT": "SUCCESS"
  }
}
```

---

## Vault configuration

```properties
vault.url=https://vault.company.ru:8200
vault.approle.conf=/opt/flink/secrets/secret.conf
vault.auth.mount=approle
vault.kv.mount=secret
vault.secret.path=datafirewall/flink/prod
vault.ssl.ca-cert.pem=/opt/flink/certs/vault-ca.pem
```

---

## CEF-аудит

CEF-аудит фиксирует технические и бизнес-события job:

- старт job;
- остановку job;
- ошибку job;
- загрузку properties;
- изменение properties hash;
- проверку подключения Kafka;
- подключение/отключение IBM MQ или Artemis;
- ошибки подключения.

```properties
cef.audit.enabled=true
cef.audit.file.enabled=true
cef.audit.kafka.enabled=true
cef.audit.fail-on-error=false

cef.audit.cef.path=/opt/datafirewall/logs/cef.log
cef.audit.kafka.bootstrap=kafka-host:9092
cef.audit.kafka.topic=datafirewall.audit

cef.audit.app.name=datafirewall-flink
cef.audit.app.version=1.0.0

cef.audit.source.port=8081
cef.audit.src.ip=10.10.1.11
cef.audit.host.name=flink-taskmanager-1
```

Kafka TLS/SASL для CEF-аудита:

```properties
cef.audit.kafka.tls.enabled=true
cef.audit.kafka.security.protocol=SASL_SSL
cef.audit.kafka.sasl.mechanism=SCRAM-SHA-512
cef.audit.kafka.ssl.truststore.location=/opt/flink/certs/truststore.p12
cef.audit.kafka.ssl.truststore.type=PKCS12
cef.audit.kafka.ssl.keystore.location=/opt/flink/certs/keystore.p12
cef.audit.kafka.ssl.keystore.type=PKCS12
cef.audit.kafka.ssl.endpoint.identification.algorithm=https
```

Приоритет Kafka-настроек для CEF-аудита:

```text
cef.audit.kafka.* -> audit.kafka.* -> kafka.*
```

---

## Примеры запуска

### IBM MQ + Flink handler

```bash
flink run \
  -p 4 \
  datafirewall-flink.jar \
  --config.file /opt/flink/conf/datafirewall-flink.properties \
  --messaging.backend mq \
  --handler flink
```

Минимальный properties:

```properties
messaging.backend=mq
handler=flink

parallelism=4
parallelism.source=4
parallelism.process=4
parallelism.sink=4

mq.host=mq-host
mq.port=1414
mq.channel=APP.SVRCONN
mq.qmgr=QM1
mq.inQueue=IN.Q
mq.outQueue=OUT.Q

kafka.bootstrap=kafka-host:9092
kafka.topic=rules-update
kafka.group=dfw-rules-group

ignite.apiUrl=http://ignite-api:8080
```

### IBM MQ + .NET handler + shadow processing

```properties
messaging.backend=mq
handler=dotnet

handler.dotnet.url=http://dotnet-service:8080/api/v1/datafirewall/process
handler.dotnet.timeout.ms=20000

handler.dotnet.shadow.kafka.enabled=true
handler.dotnet.shadow.kafka.topic=datafirewall.shadow-processing
handler.dotnet.shadow.kafka.bootstrap=kafka-host:9092

parallelism=4
parallelism.source=4
parallelism.process=4
parallelism.sink=4
parallelism.shadow.kafka=2
```

### Artemis + Flink handler

```properties
messaging.backend=artemis
handler=flink

artemis.broker.url=tcp://artemis-host:61616
artemis.in.queue=IN.Q
artemis.out.queue=OUT.Q
artemis.receive.timeout.ms=1000

parallelism=4
parallelism.source=4
parallelism.process=4
parallelism.sink=4
```

---

## Рекомендации по parallelism

Kafka source для rules update рекомендуется оставлять с `parallelism=1`, потому что далее события распространяются через broadcast stream.

Для MQ/Artemis request-response обычно имеет смысл масштабировать отдельно:

```properties
parallelism.source=4
parallelism.process=2
parallelism.sink=4
```

Если обработка легкая, а узкое место — MQ get/put или request-response latency, увеличение `parallelism.process` может почти не дать прироста. В таком случае полезнее масштабировать source/sink и количество TaskManager-процессов.

Рекомендуемые схемы:

```text
2 TaskManager x 2 slots
4 TaskManager x 1 slot
```

Для blocking I/O с IBM MQ несколько отдельных TaskManager JVM часто дают более стабильный результат, чем один TaskManager с большим количеством slots.

---

## Мониторинг и аудит

В Flink UI следует смотреть:

- `busy` по операторам;
- `backpressure`;
- `data skew`;
- records in/out per subtask;
- количество subtasks у `mq-source`, `process-with-rules-reload`, `mq-sink`.

Интерпретация:

| Симптом | Возможная причина |
|---|---|
| `busy` 90–100% на `process-with-rules-reload` | CPU/JSON/rules bottleneck. |
| `busy` высокий на `mq-sink` | Медленная отправка ответа в MQ/Artemis. |
| `data skew` высокий, `busy` низкий | Неравномерное распределение, но Flink не bottleneck. |
| `backpressure` от audit Kafka | Audit Kafka sink тормозит основной pipeline. |
| Source idle/busy низкий | Нагрузчик или MQ не поставляет достаточно сообщений. |

---

## Диагностика

### Проверка IBM MQ очередей

```bash
DISPLAY QSTATUS(IN.Q) CURDEPTH IPPROCS OPPROCS
DISPLAY QSTATUS(OUT.Q) CURDEPTH IPPROCS OPPROCS
```

### Проверка IBM MQ channel

```bash
DISPLAY CHANNEL(APP.SVRCONN) MAXINST MAXINSTC SHARECNV
DISPLAY CHSTATUS(APP.SVRCONN) ALL
```

### Частые IBM MQ reason codes

| Reason code | Значение | Возможная причина |
|---:|---|---|
| `2035` | `MQRC_NOT_AUTHORIZED` | Нет прав на queue manager/channel/queue. |
| `2085` | `MQRC_UNKNOWN_OBJECT_NAME` | Очередь или объект не найден. |
| `2397` | `MQRC_JSSE_ERROR` | TLS/JSSE ошибка, truststore/keystore/certificates. |
| `2400` | `MQRC_UNSUPPORTED_CIPHER_SUITE` | Неподдерживаемый или неверный TLS cipher suite. |

### Нагрузочное тестирование

Для корректного теста рекомендуется:

- запускать нагрузчик на отдельном сервере;
- тестировать не менее 3–5 минут;
- использовать достаточное количество in-flight запросов;
- сравнивать не только RPS, но и `avg`, `p95`, `p99` latency;
- отдельно проверять `parallelism.source`, `parallelism.process`, `parallelism.sink`.

Пример матрицы:

```text
threads: 20 / 50 / 100 / 200
parallelism: 1 / 2 / 4 / 8
requests: 50 000+
```

---

## Сборка

```bash
mvn clean package
```

После сборки jar можно запускать через Flink CLI:

```bash
flink run -p 4 target/datafirewall-flink.jar --config.file /opt/flink/conf/datafirewall-flink.properties
```

---

## Краткий чек-лист перед запуском

- [ ] Заполнены Vault параметры.
- [ ] Для `handler=dotnet` в Vault задан `dotnetJwt`.
- [ ] Проверен доступ к IBM MQ или Artemis.
- [ ] Проверены права MQ user на входную и выходную очереди.
- [ ] Проверены Kafka topic для rules update.
- [ ] Если включен audit/detail — проверен Kafka topic для audit.
- [ ] Если включен shadow processing — проверен Kafka topic `handler.dotnet.shadow.kafka.topic`.
- [ ] Проверен доступ к Ignite API.
- [ ] Проверены TLS truststore/keystore paths на всех TaskManager hosts.
- [ ] В Flink UI проверено, что source/process/sink имеют ожидаемый parallelism.
