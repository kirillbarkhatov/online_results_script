# Online Results

Скрипт делает "живую" трансляцию результатов из Google Sheets:
- читает все листы;
- парсит группы и строки спортсменов;
- раз в секунду сравнивает снапшоты;
- пишет в SQLite только изменения;
- выводит в консоль обновления и текущую расстановку мест по активной группе.

## Подготовка

1. Создайте сервисный аккаунт Google Cloud.
2. Включите Google Sheets API.
3. Скачайте JSON-ключ сервисного аккаунта.
4. Дайте сервисному аккаунту доступ `Viewer` к таблице.
5. Заполните `.env` на основе `.env.example`.

## Запуск

```bash
poetry install
poetry run python -m online_results.main
```

Проверка подключения одним чтением:

```bash
poetry run python -m online_results.main --once
```

## Запуск в Docker

1. Укажите путь до JSON ключа сервисного аккаунта:

```bash
export GOOGLE_SERVICE_ACCOUNT_FILE_HOST=/absolute/path/to/service-account.json
```

2. Поднимите API:

```bash
docker compose up -d --build
```

3. Проверка:

```bash
curl http://localhost:8002/health
```

## API режим (запуск воркера по ссылке)

Запуск API:

```bash
poetry run uvicorn online_results.api_app:app --host 0.0.0.0 --port 8002
```

Создать поток мониторинга по ссылке на протокол:

```bash
curl -X POST http://localhost:8002/v1/streams \
  -H "Content-Type: application/json" \
  -d '{
    "protocol_link": "https://docs.google.com/spreadsheets/d/<ID>/edit",
    "poll_interval_sec": 1.0,
    "finalize_timeout_sec": 300,
    "finalize_max_missing": 2,
    "callback_url": "https://external.system/webhook",
    "callback_secret": "secret",
    "worker_print": false
  }'
```

Статус потока:

```bash
curl http://localhost:8002/v1/streams/<stream_id>
```

Остановить поток:

```bash
curl -X POST http://localhost:8002/v1/streams/<stream_id>/stop
```

Live события без polling:

1. SSE:

```bash
curl -N http://localhost:8002/v1/streams/<stream_id>/events
```

2. WebSocket:

```text
ws://localhost:8002/ws/streams/<stream_id>
```

Тест WebSocket:

1. В Postman:
`New -> WebSocket Request -> ws://localhost:8002/ws/streams/<stream_id>`

2. Через `wscat`:

```bash
npm i -g wscat
wscat -c ws://localhost:8002/ws/streams/<stream_id>
```

3. В браузере (DevTools):

```javascript
const ws = new WebSocket("ws://localhost:8002/ws/streams/<stream_id>");
ws.onmessage = (e) => console.log(JSON.parse(e.data));
```

Postman файлы для API-тестов в корне проекта:
- `online_results_api.postman_collection.json`
- `online_results_api.postman_environment.json`

Исторические результаты (после завершения стримов):

1. Список соревнований из БД:

```bash
curl "http://localhost:8002/v1/results/events"
```

2. Финальные результаты по событию:

```bash
curl "http://localhost:8002/v1/results/final?event_date=04.03.2026&event_name=КУБОК%20ФЕДЕРАЦИИ%20ЛЕНИНГРАДСКОЙ%20ОБЛАСТИ"
```

3. Свод мест по спортсменам (Фамилия Имя -> дата: место/статус):

```bash
curl "http://localhost:8002/v1/results/athlete-places"
```

## Структура базы SQLite

- `athletes`: карточка спортсмена и принадлежность к группе.
- `current_results`: актуальный снимок полей `1 заезд`, `2 заезд`, `Результат`.
- `snapshots`: информация о каждом тике, где были изменения.
- `result_updates`: журнал только изменившихся записей.
