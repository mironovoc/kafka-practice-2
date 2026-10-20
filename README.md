# Практическая работа 2

Java, клиент `kafka-clients`. Кластер из ПР1 - три брокера в `docker-compose.yml`, приложение в папке `kafka-practice2`.

## Что где лежит

- `docker-compose.yml` - ZooKeeper, kafka-1/2/3, Kafka UI, сервис `kafka-app`
- `Dockerfile` - собирает JAR и запускает приложение
- `kafka-practice2` - основной код
  - `model/EventMessage` - поля сообщения
  - `serde/EventMessageSerde` - JSON через Jackson
  - `producer/MessageProducer` - отправка в топик
  - `consumer/SingleMessageConsumer` - читает по одному, автокоммит
  - `consumer/BatchMessageConsumer` - пачка от 10 штук, потом `commitSync()`
- `topic.txt` - команда создания топика и вывод `--describe`

Spring Kafka не использовал - только API из уроков.

## Как устроено приложение

При старте (`KafkaPractice2Application`) поднимаются три потока: продюсер и два консьюмера.

Продюсер раз в ~300 мс создаёт `EventMessage`, пишет JSON в консоль и вызывает `send()` (асинхронно). В Docker в `sourceInstance` попадает hostname контейнера - по нему видно, что работают два экземпляра.

`SingleMessageConsumer` - группа `single-message-consumer-group`, `max.poll.records=1`, `enable.auto.commit=true`.

`BatchMessageConsumer` - группа `batch-message-consumer-group`, автокоммит выключен, коплю минимум 10 записей через несколько `poll`, обрабатываю в цикле, один раз `commitSync()`. Для fetch выставил `fetch.min.bytes` и `fetch.max.wait.ms` (смотри комментарии в классе).

У Single и Batch разные `group_id`, поэтому оба типа читают одни и те же сообщения из топика, но со своими offset.

## Запуск (как делал у себя)

Команды из каталога репозитория, где лежит `docker-compose.yml` (у меня `D:\kaf\1`). Если запустить `docker compose` из другой папки - будет ошибка «no configuration file provided».

1. Кластер:

```
docker compose up -d zookeeper kafka-1 kafka-2 kafka-3 ui
docker compose ps
```

UI: http://localhost:8080

2. Топик (если уже есть - create ругнётся, тогда только describe):

```
docker compose exec kafka-1 kafka-topics --create --topic my-topic --partitions 3 --replication-factor 2 --bootstrap-server kafka:9092
docker compose exec kafka-1 kafka-topics --describe --topic my-topic --bootstrap-server kafka:9092
```

Результат describe - в `topic.txt`. После `docker compose down` и нового `up` у топика будет другой `TopicId`, describe нужно снять заново.

3. Сборка и два экземпляра приложения:

```
docker compose build kafka-app
```

Первый build долгий: внутри контейнера качается Gradle и зависимости, минуты 3-5 - это нормально.

```
docker compose up -d --scale kafka-app=2 kafka-app
docker compose logs -f kafka-app
```

`deploy.replicas: 2` в compose у меня для задания; в обычном Docker Compose масштабирование через `--scale kafka-app=2`.

4. Локально без Docker для app (опционально):

```
cd kafka-practice2
gradlew.bat bootJar
set BOOTSTRAP_SERVERS=localhost:9092
java -jar build\libs\kafka-practice2.jar
```

С Windows на `localhost:9092` у меня не пробовал - гонял app в контейнере, там `BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092`.

## Как проверял

- В describe: `PartitionCount: 3`, `ReplicationFactor: 2`
- В логах `[Producer] Отправка:` - JSON уходит в топик
- `[SingleMessageConsumer] Получено:` - по одной строке на poll
- `[BatchMessageConsumer]` и `закоммичена пачка из 10 сообщений` (или больше)
- Один и тот же `payload` (например event-100) есть и в Single, и в Batch - разные группы
- `docker compose ps` - два контейнера `kafka-app`, в сообщениях разные `sourceInstance` (два hostname)
- Гарантии доставки: в продюсере `acks=all`, `retries`, idempotence - см. `MessageProducer`

Остановка: `docker compose down`
