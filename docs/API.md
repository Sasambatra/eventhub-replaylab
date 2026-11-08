# API первой версии

**Русский** · [English](API.en.md)

Авторизация: `Authorization: Bearer <key>` или same-origin browser session.
Ключи не помещают в URL. Для cookie-записей обязательны Origin и X-CSRF-Token.
Документация схемы: локальный `/docs` и `/openapi.json`; реальные операции требуют прав.

| Метод / путь | Назначение | Scope |
| --- | --- | --- |
| POST /v1/events | Приём; 202 новый, 200 повтор, 409 конфликт | events:write + source |
| GET /v1/events | Поиск, limit 1–100, подписанный cursor | events:read |
| GET /v1/events/{id} | Событие, hash, состояние retention | events:read |
| GET /v1/events/{id}/deliveries | Получатели и отдельные попытки | events:read |
| POST /v1/deliveries/{id}/retry | Новый delivery run, нужен Idempotency-Key | deliveries:retry |
| GET /v1/destinations | Только цели проекта, без секретов | events:read |
| POST /v1/destinations | name + adapter lab-a/lab-b | destinations:manage |
| PATCH /v1/destinations/{id} | active true/false | destinations:manage |
| GET /v1/cases | Определения лабораторных сценариев | replays:read |
| POST /v1/cases/import | Строгий case или {case,checksum} | cases:manage |
| POST /v1/cases/capture | {event_id,pattern}, arrival/reorder_duplicate | cases:manage + events:read |
| GET /v1/cases/{id}/export | Обезличенная копия с checksum | cases:export |
| POST /v1/replays | {case_id}, нужен Idempotency-Key | replays:run |
| GET /v1/replays | Последние 25 запусков | replays:read |
| GET /v1/replays/{id}/report | Состояние, наблюдения, правила, итог | replays:read |
| POST /v1/replays/{id}/cancel | Сохранить запрос отмены | replays:run |
| GET /v1/overview, /v1/metrics, /v1/stream | Сводка, scoped gauges, SSE | events:read |
| GET/POST /v1/keys | Список/выдача ключа один раз | keys:manage, owner |
| POST /v1/keys/{id}/revoke | Отзыв ключа и связанных сессий | keys:manage |

Одинаковый Idempotency-Key одной операции возвращает исходный run. Изменение
назначения с тем же ключом вызывает 409. Повтор exhausted/accepted/cancelled
создаёт отдельный run, event ID остаётся прежним; активный run нельзя перезапустить.
Чужой project ID не раскрывается: запись выглядит отсутствующей. Ключ source
не получает чтение или управление. Срок нового делегированного ключа 90 дней,
можно задать `expires_in_days` 1–365. Viewer ограничен чтением/экспортом;
operator управляет сценариями/целями/повторами, но не выдаёт ключи и не пишет события.

CloudEvents-профиль: `lab/cases/*.json`. Поддерживается только
`studio.booking.snapshot.v1`, `source` вида urn, `subject=booking/<UUID>`,
положительная целая revision, status held/confirmed/cancelled/expired,
время с timezone. Body события до 64 KiB, сценария до 1 MiB. Сценарий: до 500
определений/2000 шагов одного объекта, явные ожидания A/B; без произвольного кода/URL.
Служебный контракт RabbitMQ не является публичным приёмом событий.

Для программной демонстрации используется CLI, который читает ключ из приватного
файла, а не из аргумента процесса:

```bash
.venv/bin/python -m eventhub demo --output .local/reports/demo.json
.venv/bin/python -m eventhub verify-case --case lab/cases/timeout_after_commit.json
```

`demo` импортирует сценарий, отправляет его обычные события и запускает replay.
`verify-case` только проверяет сценарий в лаборатории. Exit 0 означает completed
и совпадение с заявленными ожиданиями, включая ожидаемый fail контрольной версии A.
