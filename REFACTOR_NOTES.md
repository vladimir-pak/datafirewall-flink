# Refactor: cache runtime + dynamic handler routing

## Что изменено

1. `RulesReloadBroadcastProcessFunction` стал тонким Flink-orchestrator:
   - lifecycle `open/close`;
   - обработка Kafka broadcast-событий;
   - runtime routing по `handler`;
   - делегирование в `MessageProcessingService` или `DotnetHandlerClient`.

2. Вынесен `RulesCacheRuntime`:
   - инициализация rules loader;
   - startup bootstrap кэшей;
   - reload `compiled_rules`;
   - reload politics bundle;
   - предоставление snapshot-ов для обработки сообщений.

3. Вынесен `MessageProcessingService`:
   - вся текущая Flink-обработка сообщения;
   - нормализация входного JSON;
   - валидация;
   - формирование short/detail answers;
   - сборка `ProcessingResult`.

4. Добавлен `DotnetHandlerClient`:
   - отправляет исходное MQ/JMS payload тело в dotnet API через HTTP POST;
   - ответ API считается готовым short response и отправляется обратно в MQ/JMS sink;
   - `detailJson` для dotnet-ветки сейчас `null`.

5. Добавлен динамический handler:
   - значения: `flink`, `dotnet`;
   - default берется из properties: `handler`, fallback `handler.default`, fallback `flink`;
   - runtime переключение через Kafka broadcast topic.

## Properties

```properties
handler=flink
# или
handler.default=flink

handler.dotnet.url=http://dotnet-service:8080/api/v1/datafirewall/process
handler.dotnet.timeout.ms=20000
# опционально, если отличается от vaultSecrets.jwt()
handler.dotnet.jwt=eyJ...
```

## Kafka event для переключения handler в runtime

```json
{
  "cacheName": "handler",
  "version": 1,
  "handler": "dotnet"
}
```

Вернуть обработку во Flink:

```json
{
  "cacheName": "handler",
  "version": 2,
  "handler": "flink"
}
```

`version` используется как monotonic guard: событие с версией меньше или равно текущей игнорируется.

## Kafka events для кэшей остались прежними

```json
{"cacheName":"compiled_rules","version":123}
{"cacheName":"politics","version":123}
```

## Dotnet API contract

Текущая реализация отправляет в dotnet API **сырой JSON payload** из MQ/JMS:

```http
POST handler.dotnet.url
Content-Type: application/json; charset=utf-8
Authorization: Bearer <jwt>

<original MQ/JMS payload>
```

Ответ dotnet API должен быть JSON-строкой, которую можно сразу отправить как short answer обратно в MQ/JMS.
