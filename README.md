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

--rules.loader=http
--ignite.apiUrl=http://127.0.0.1:8080
--kafka.bootstrap=localhost:9092
--kafka.topic=rules-update
--kafka.group=dfw-rules-group
--use.mq=false
--cache.bootstrap.enabled=true
--politics.bootstrap.enabled=false
--log.payloads=true
--test.json.path=/Users/kampus/Downloads/query_full.json
--mq.host=localhost
--mq.port=1414
--mq.channel=DEV.APP.SVRCONN
--mq.qmgr=QM1
--mq.inQueue=TEST.QUEUE
--mq.outQueue=REPLY.QUEUE
--mq.user=admin
--mq.password=admin123
--test.politic.caches.path=/Users/kampus/Downloads/test-caches.json
--test.politic.caches.enabled=true
--parallelism.source=1
--parallelism.kafka=1
--parallelism.process=1
--parallelism.sink=1
--detail.kafka.enabled=true
--detail.kafka.bootstrap=localhost:9092
--detail.kafka.topic=detail-answer


##  Параметры запуска описание


--rules.loader=http \                          # Источник правил (http или thin)
--ignite.apiUrl=http://127.0.0.1:8080 \        # URL Ignite API

--kafka.bootstrap=localhost:9092 \             # Kafka broker
--kafka.topic=rules-update \                   # Топик обновления кэшей
--kafka.group=dfw-rules-group \                # Consumer group

--use.mq=false \                               # false = читаем из файла тестовый режим, true = из MQ

--cache.bootstrap.enabled=true \               # Загружать кэши при старте
--politics.bootstrap.enabled=false \           # Загружать politics-кэши при старте

--log.payloads=true \                          # Логировать payload (с маской) для скрытия данных

--test.json.path=/Users/kampus/Downloads/query_full.json \  # Путь к тестовому  входному JSON если запустить без MQ 

--mq.host=localhost \                           # MQ хост
--mq.port=1414 \                               # MQ порт
--mq.channel=DEV.APP.SVRCONN \                 # MQ канал
--mq.qmgr=QM1 \                                # Queue manager

--mq.inQueue=TEST.QUEUE \                      # Очередь входящих сообщений (используется только при use.mq=true)
--mq.outQueue=REPLY.QUEUE \                    # Очередь для отправки ответов

--mq.user=admin \                              # Пользователь MQ
--mq.password=admin123                         # Пароль MQ
--test.politic.caches.path=/Users/kampus/Downloads/test_politic_caches.json # путь к файлу с тестовыми политик кешами если не инициализировать через иннайт
--test.politic.caches.enabled=true                     # использовать ли тестовое заполнение политик кешей





