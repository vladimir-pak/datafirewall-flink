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
--kafka.tls.enabled=false
--kafka.security.protocol=SSL
--kafka.ssl.truststore.location=
--kafka.ssl.truststore.password=
############################
#
KAFKA
(DETAIL
ANSWER)
############################
--audit.kafka.enabled=false
--audit.kafka.bootstrap=localhost:9092
--audit.kafka.topic=detail-answer
--audit.kafka.tls.enabled=false
--audit.kafka.security.protocol=SSL
--audit.kafka.ssl.truststore.location=
--audit.kafka.ssl.truststore.password=
############################
#
ARTEMIS
############################
--artemis.broker.url=tcp://localhost:61616
--artemis.user=admin
--artemis.password=admin
--artemis.in.queue=IN.Q
--artemis.out.queue=OUT.Q
--artemis.receive.timeout.ms=1000
#
TLS
--artemis.tls.enabled=false
--artemis.ssl.truststore.location=
--artemis.ssl.truststore.password=
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
--mq.user=admin
--mq.password=admin123
--mq.wait.interval.ms=1000
#
TLS
--mq.tls.enabled=false
--mq.tls.cipherSuite=
--mq.ssl.truststore.location=
--mq.ssl.truststore.password=


