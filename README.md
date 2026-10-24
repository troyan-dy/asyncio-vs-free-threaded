# asyncio vs free-threaded Python: что даёт отказ от GIL веб-сервису

Набор воспроизводимых экспериментов: один и тот же небольшой сервис с I/O- и CPU-работой в хендлерах
и с фоновыми задачами, запущенный в разных моделях конкурентности на обычной и free-threaded сборках
CPython 3.14, под одинаковой нагрузкой в Docker с лимитом 4 CPU.

Отчёт с полными таблицами и оговорками: **[RESULTS.md](RESULTS.md)**.

## Что измеряется

Сервис на FastAPI ([fastapi_app.py](fastapi_app.py)) с хендлером `/quote`:

1. поход в «базу» через пул соединений: echo-сервер отвечает с задержкой (1 мс в лёгком профиле, 20 мс в тяжёлом);
2. чистый Python CPU: цикл на `cpu_iters` итераций (20 000 ≈ 0.6 мс, 100 000 ≈ 3.5 мс);
3. JSON-ответ, в который попадают версии фонового стейта.

Фоном в том же процессе работают три задачи, меняющие общий стейт: перечитывание `config.json` по
mtime, разбор push-стрима (аналог gRPC-стрима, сообщение каждые 10 мс), ротация всех соединений пула
каждые 3 с. Нагрузочный генератор проверяет, что всё это продолжает обновляться под нагрузкой.

Конфигурации (каждая на GIL-сборке и на free-threaded):

| конфигурация | как запущено |
|---|---|
| async 1 loop | `uvicorn_serve.py --loops 1`, uvloop + httptools |
| async 4 loops в потоках | 4 event loop'а в 4 потоках одного процесса, у каждого свой `SO_REUSEPORT`-сокет |
| async 4 процесса | 4 процесса, у каждого свой `SO_REUSEPORT`-сокет |
| async + CPU в `to_thread` | CPU-часть через `asyncio.to_thread`, размер пула задаётся `EXECUTOR_THREADS` |
| sync FastAPI `def` | `def`-хендлер в пуле anyio на 64 потока; `CPU_PERMITS=N` ограничивает число одновременно считающих |

Сервер работает в контейнере с `--cpus 4`, бэкенды (`--cpus 2`) и генератор нагрузки (`--cpus 3`,
free-threaded сборка, 64 реально параллельных потока) в отдельных контейнерах той же сети.

## Запуск

Нужны Docker и bash. Образ собирает оба интерпретатора через uv (3.14 и 3.14t) и ставит зависимости,
первая сборка занимает несколько минут.
Версии пакетов зафиксированы в `requirements.txt`; после правки `requirements.in` он перегенерируется так:
`uv pip compile requirements.in -o requirements.txt --python-version 3.14 --universal`.

```bash
./run_docker_matrix.sh                                  # лёгкий профиль: база 1 мс, 20k итераций
ECHO_DELAY=0.02 CPU_ITERS=100000 ./run_docker_matrix.sh # тяжёлый профиль: база 20 мс, 100k итераций
python3 print_results.py results/docker_results.jsonl   # таблица: лучший прогон на конфигурацию
```

Переменные окружения: `CONC` (соединений, 64), `DUR` (секунд на конфигурацию, 10), `SERVER_CPUS` (4),
`ECHO_DELAY`, `CPU_ITERS`, `OUT` (файл результатов). Полная матрица из 12 конфигураций занимает около
4 минут плюс сборка образа.

Одна конфигурация:

```bash
APP_SYNC=1 CPU_PERMITS=4 ./docker_bench_one.sh "sync, 4 permits" .venv-ft/bin/python uvicorn_serve.py --loops 1
APP_CPU=thread EXECUTOR_THREADS=4 ./docker_bench_one.sh "to_thread(4)" .venv-ft/bin/python uvicorn_serve.py --loops 1
./docker_bench_one.sh "4 procs, gil" .venv-gil/bin/python uvicorn_serve.py --workers 4
docker rm -f backends   # бэкенды остаются запущенными между вызовами
```

Воспроизвести находку про TCP_NODELAY: `./probe_nodelay.sh`.

### Линтер

Код проверяется [wemake-python-styleguide](https://wemake-python-styleguide.readthedocs.io/) (настройки в
`setup.cfg`, в CI это workflow `lint`). Dev-зависимости зафиксированы в `requirements-dev.txt`, после
правки `requirements-dev.in` он перегенерируется так:
`uv pip compile requirements-dev.in -o requirements-dev.txt --python-version 3.14 --universal`.

```bash
uv pip install -p .venv-gil/bin/python -r requirements-dev.txt
.venv-gil/bin/flake8 . --select=WPS
```

Точечные `# noqa: WPSxxx` стоят только в горячем пути (хендлеры `/quote`, `cpu_work`, измерительный цикл
`loadgen.py`, последовательная ротация пула) и там, где форма кода задаёт семантику бенчмарка
(последовательное открытие пулов, писатель конфига в отдельном потоке). У каждого записана причина.

## Главные результаты

Лёгкий профиль (база 1 мс + 0.6 мс CPU), 64 соединения, лучший из прогонов, 4 CPU:

| конфигурация | GIL rps | free-threaded rps | p99 ft | память ft |
|---|---|---|---|---|
| async 1 loop | 1 620 | 1 562 | 88 мс | 76 MB |
| async 4 loop'а в потоках | 1 375 | 3 326 | 53 мс | 101 MB |
| async 4 процесса | **6 213** | 5 443 | 24 мс | 241 MB |
| async 1 loop + to_thread | 1 390 | 2 525 | 74 мс | 102 MB |
| sync `def`, 64 потока | 1 362 | 2 895 | 71 мс | 347 MB |

Тяжёлый профиль (база 20 мс + 3.5 мс CPU):

| конфигурация | GIL rps | free-threaded rps | p99 ft | CPU мс на запрос, ft |
|---|---|---|---|---|
| async 4 процесса | **932** | 882 | 131 | 3.4 |
| async 4 loop'а в потоках | 307 | 661 | 215 | 4.7 |
| async 1 loop + to_thread, пул 14 (дефолт) | 297 | 473 | 277 | 8.6 |
| async 1 loop + to_thread, пул 4 | | **676** | **126** | 5.9 |
| sync `def`, 64 потока | 279 | 576 | 217 | 7.1 |
| sync `def`, 64 потока, считают не больше 4 | 284 | **725** | 169 | 5.5 |

Выводы коротко:

- Лучший вариант на 4 CPU в обоих профилях это 4 процесса на обычной GIL-сборке. Free-threaded его не обогнал.
- Free-threaded стоит 10–25 % на поток даже в процессах, а при 4 параллельных интерпретаторных потоках
  один запрос обходится в 1.5 раза дороже по CPU, чем в процессах.
- Sync-код на free-threaded ускоряется в 2 раза без правок, но стоит в 6 раз больше памяти (per-thread аллокатор).
- Число одновременно считающих потоков нужно держать равным числу CPU: семафор вокруг CPU-части или пул
  `to_thread` по числу ядер дают +30–45 % rps и вдвое лучшие хвосты. Больше ядер уже вредит.
- Побочная находка: на Linux в CPython 3.14 `asyncio` `create_server(sock=...)` оставляет accepted-сокеты
  без TCP_NODELAY, и uvicorn с `--loop asyncio` и внешним сокетом (в том числе `--workers`) получает
  задержку 40 мс между заголовками и телом ответа. uvloop не подвержен.

Подробности, таблицы со всеми прогонами и оговорки в [RESULTS.md](RESULTS.md). Числа из VM Docker Desktop
на Apple M2 Pro, разброс между прогонами до 20 %. Они сняты до рефакторинга под wemake-python-styleguide
(код на коммите `c7120b3`); после него ответы эндпоинтов те же, в горячем пути не добавилось вызовов,
но полная матрица в Docker заново не прогонялась.

## Файлы

| файл | назначение |
|---|---|
| `fastapi_app.py`, `uvicorn_serve.py` | сервис на FastAPI (выбор варианта по `APP_SYNC`) и лаунчер (loops в потоках / процессы) |
| `async_variant.py`, `async_pool.py`, `async_tasks.py` | `async def`-вариант: хендлер и lifespan, пул на asyncio streams по одному на event loop, фоновые задачи |
| `sync_variant.py` | `def`-вариант: хендлер из `demo_app` в пуле потоков anyio |
| `demo_app.py`, `miniflask.py`, `rt_threads.py` | логика хендлера и фоновых задач, пул соединений на блокирующих сокетах для sync-варианта |
| `backends.py`, `echo_server.py`, `backend_protocol.py` | заглушки: «база» с задержкой, push-стрим, писатель конфига; общие порты и формат сообщений |
| `loadgen.py`, `loadgen_report.py`, `print_results.py` | генератор нагрузки с перцентилями и проверкой свежести стейта; печать таблиц |
| `run_docker_matrix.sh`, `docker_bench_one.sh`, `Dockerfile` | запуск в Docker |
| `requirements.in`, `requirements.txt` | зависимости и их зафиксированные версии (uv pip compile) |
| `requirements-dev.in`, `requirements-dev.txt`, `setup.cfg` | линтер wemake-python-styleguide и его настройки |
| `nodelay_check.py`, `stall_timing.py`, `probe_nodelay.sh` | диагностика TCP_NODELAY |
| `results/` | сырые результаты всех прогонов (JSON lines + sidecar с памятью и CPU контейнера) |

Код экспериментальный: без тестов и обработки ошибок сверх необходимого для запуска. Стиль проверяет
wemake-python-styleguide (см. «Линтер»).
