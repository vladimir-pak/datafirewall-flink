# DataFirewallFlink

## Описание
DataFirewallFlink — приложение для обработки и фильтрации потоковых данных на базе Apache Flink.
Система предназначена для анализа, валидации и маршрутизации входящих сообщений в режиме реального времени с поддержкой динамически обновляемых бизнес-правил.

## Технологии
- Java 17
- Apache Flink
- Apache Kafka
- IBM MQ
- Apache Ignite
- Maven
- Git

## Архитектура:
Приложение реализовано в виде одной Flink Job, которая выполняет:
- загрузку правил при старте приложения;
- обработку сообщений из IBM MQ;
- отправляет ответ после обработки в IBM MQ;
- отправляет детальный ответ после обработки в Kafka
- обновление правил(классы RuleXXX) по событиям из Kafka.

Архитектура обеспечивает:
- параллельную обработку сообщений;
- атомарное обновление правил;
- отсутствие промежуточных неконсистентных состояний;
- горизонтальную масштабируемость.

## Обработка сообщений (IBM MQ)
- Используется одна входная очередь IBM MQ.
- MqSource запускается с parallelism = N.
- Каждое сообщение обрабатывается ровно одним subtask.
- Subtask распределяются между TaskManager’ами Flink.
Обработка полностью параллельная и масштабируется увеличением parallelism.

## Обновление правил (Kafka)
- Топик: rules-update
- Пример сообщения:
  {"version": 4}
- Kafka Source работает с parallelism = 1.
- Событие обновления передаётся через Broadcast всем subtask.

## Механизм обновления 
При получении события обновления каждый subtask:
- Загружает новую версию правил через Ignite API.
- Компилирует правила.
- Атомарно обновляет локальный реестр правил.
- Правила хранятся в виде:
  AtomicReference<Map<String, Rule>> (key:название класса, value: класс)
  что гарантирует после успешной компиляции ссылка заменяется целиком, что гарантирует:
  - отсутствие промежуточного состояния
  - использование либо старой, либо новой версии правил
  - отсутствие блокировок

## Ключевые свойства
- Одна Flink Job
- Параллельная обработка сообщений
- Динамическое обновление без перезапуска
- Broadcast-распространение обновлений
- Атомарное переключение версии правил
- Горизонтальная масштабируемость
Архитектура обеспечивает консистентную и масштабируемую потоковую обработку с динамическим управлением бизнес-логикой.


##  Параметры запуска

############################
#
BACKEND
SWITCH
############################
--messaging.backend=artemis
#
значения:
mq
|
artemis
############################
#
RULES
/
CACHE
############################
--rules.loader=http
--ignite.apiUrl=http://127.0.0.1:8080
--ignite.api.tls.ca.pem=/opt/flink/certs/ca.pem
--cache.bootstrap.enabled=true
--politics.bootstrap.enabled=false
--test.politic.caches.enabled=true
--test.politic.caches.path=/Users/kampus/Downloads/test_politic_caches.json
############################
#
LOGGING
############################
--log.payloads=true
--log.preview.len=600
############################
#
KAFKA
(RULES)
############################
--kafka.bootstrap=localhost:9092
--kafka.topic=rules-update
--kafka.group=dfw-rules-group
#
TLS
(включать
только
если
надо)
--kafka.tls.enabled=true
--kafka.security.protocol=SASL_SSL
--kafka.sasl.mechanism=SCRAM-SHA-512
--kafka.ssl.truststore.location=/opt/flink/certs/truststore.p12
--kafka.ssl.truststore.type=PKCS12
--kafka.ssl.keystore.location=/opt/flink/certs/keystore.p12
--kafka.ssl.keystore.type=PKCS12
--kafka.ssl.endpoint.identification.algorithm=https
# kafkaUser/kafkaPassword, truststorePassword, keystorePassword берутся из VaultSecretsDto
############################
#
KAFKA
(DETAIL
ANSWER)
############################
--audit.kafka.enabled=false - required
--audit.kafka.bootstrap=localhost:9092 - required
--audit.kafka.topic=detail-answer - required
--audit.kafka.tls.enabled=true
--audit.kafka.security.protocol=SASL_SSL
--audit.kafka.sasl.mechanism=SCRAM-SHA-512
--audit.kafka.ssl.truststore.location=/opt/flink/certs/truststore.p12
--audit.kafka.ssl.truststore.type=PKCS12
--audit.kafka.ssl.keystore.location=/opt/flink/certs/keystore.p12
--audit.kafka.ssl.keystore.type=PKCS12
--audit.kafka.ssl.endpoint.identification.algorithm=https
# Если audit.kafka.* не задан, параметры подключения наследуются из kafka.*
############################
#
ARTEMIS
############################
--artemis.broker.url=tcp://localhost:61616
--artemis.in.queue=IN.Q
--artemis.out.queue=OUT.Q
--artemis.receive.timeout.ms=1000
#
TLS
--artemis.tls.enabled=false
--artemis.ssl.truststore.location=
--artemis.ssl.keystore.location=
--artemis.ssl.enabled.cipher.suites=
############################
#
IBM
MQ
############################
--mq.host=localhost
--mq.port=1414
--mq.channel=DEV.APP.SVRCONN
--mq.qmgr=QM1
--mq.inQueue=IN.Q
--mq.outQueue=OUT.Q
--mq.wait.interval.ms=1000
#
TLS
--mq.tls.enabled=false
--mq.tls.cipherSuite=
--mq.ssl.truststore.location=


#
Vault
--vault.url=https://vault.company.ru:8200
--vault.approle.conf=/opt/flink/secrets/secret.conf
--vault.auth.mount=approle
--vault.kv.mount=secret
--vault.secret.path=datafirewall/flink/prod
--vault.ssl.ca-cert.pem=/opt/flink/certs/vault-ca.pem

#
CEF логирование
############################
cef.audit.enabled=true
cef.audit.file.enabled=true
cef.audit.kafka.enabled=true
cef.audit.fail-on-error=false

cef.audit.cef.path=/opt/datafirewall/logs/cef.log
cef.audit.kafka.bootstrap=kafka-host:9092
cef.audit.kafka.topic=datafirewall.audit

cef.audit.app.name=datafirewall-flink - если не задан, берется из имени jar
cef.audit.app.version=1.0.0 - если не задан, берется из имени jar

cef.audit.source.port=8081 - порт, где работает flink
cef.audit.src.ip=10.10.1.11 - не обязательно, будет искать InetAddress.getLocalHost().getHostAddress();
cef.audit.host.name=flink-taskmanager-1

Необязательные
cef.audit.kafka.tls.enabled=true
cef.audit.kafka.security.protocol=SASL_SSL
cef.audit.kafka.sasl.mechanism=SCRAM-SHA-512
cef.audit.kafka.ssl.truststore.location=/opt/flink/certs/truststore.p12
cef.audit.kafka.ssl.truststore.type=PKCS12
cef.audit.kafka.ssl.keystore.location=/opt/flink/certs/keystore.p12
cef.audit.kafka.ssl.keystore.type=PKCS12
cef.audit.kafka.ssl.endpoint.identification.algorithm=https
# Для CEF-аудита приоритет такой: cef.audit.kafka.* -> audit.kafka.* -> kafka.*
# Пароли и логин Kafka берутся из VaultSecretsDto: kafkaUser, kafkaPassword, truststorePassword, keystorePassword
############################
