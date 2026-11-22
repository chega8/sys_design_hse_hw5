# ДЗ 5. Мониторинг антифрода

За основу взял код своей [третьей домашней](https://github.com/chega8/sys_design_hse_hw3). Симулятор непрерывно отправляет транзакции в Kafka. CatBoost считает `score` и `fraud_flag` на CPU, а писатель сохраняет результаты в PostgreSQL. Prometheus собирает метрики раз в секунду.

## Запуск

Нужны Docker с Compose и Python 3. Из корня репозитория:

```bash
cp .env.example .env
python3 scripts/demo_data.py
docker compose up -d --build
docker compose ps -a
```

Демоданные синтетические. Чтобы запустить с датасетом из прошлой работы, укажите путь к `train.csv` в `TRAIN_DATA_CSV` в файле `.env` перед первым запуском.

- Grafana: [localhost:3000](http://localhost:3000), логин `admin`, пароль `fraud-demo`.
- Prometheus: [localhost:9090](http://localhost:9090), вкладка [Targets](http://localhost:9090/targets).
- Airflow: [localhost:8081](http://localhost:8081), логин и пароль `admin`.

## Что смотреть

| Дашборд | Панели |
| --- | --- |
| [Обработка транзакций](http://localhost:3000/d/fraud-scoring) | TPS, p95 обработки, доля фрода за 30 секунд, ошибки, тепловая карта скоров. На TPS появляется отметка смены модели. |
| [Запись результатов](http://localhost:3000/d/fraud-writes) | Скорость записи, p95 INSERT с коммитом, счётчик записанных фродов и тепловая карта скоров. |
| [Системные метрики](http://localhost:3000/d/fraud-host) | CPU, память, сетевые байты, дисковый ввод-вывод и место на диске. В Docker на macOS CPU, память и диск относятся к Linux-окружению Docker, а сетевые метрики сняты в сети контейнера экспортера. |

На дашборде обработки есть таблица с числом скоров и долей фрода по версиям модели. Фильтры **Штат** и **Мерчант** меняют плотность скоров и TPS по данным из `scores`. Доля фрода по `cat_id` считается на последних 1000 записях. Рядом показана сумма операций, помеченных фродом, по категориям за выбранный период.

| Сервис | Метрики |
| --- | --- |
| [Скоринг](http://localhost:8000/metrics) | `fraud_scored_total`, `fraud_flagged_total`, `fraud_ratio` (доля с запуска), гистограммы `fraud_processing_seconds` и `fraud_score`, `fraud_scoring_errors_total`, `fraud_model_reloads_total{version}`, `fraud_model_reloaded_at_seconds{version}`. |
| [Запись](http://localhost:8001/metrics) | `fraud_written_total`, `fraud_written_flagged_total`, гистограммы `fraud_write_seconds` и `fraud_written_score`. |
| Node Exporter | `node_cpu_seconds_total`, `node_memory_Mem{Available,Total}_bytes`, `node_network_*`, `node_disk_*`, `node_filesystem_*`. |

Добавил гистограмму `fraud_preprocessing_seconds`. На панели **Время обработки и подготовки признаков (p95)** видно, растёт ли время подготовки признаков вместе с общим временем скоринга.

## Проверка

На странице Targets все три сервиса должны перейти в `UP`. Откройте `fraud_scored_total` и `fraud_written_total` в Prometheus или дважды выполните команду с паузой в несколько секунд. Оба счётчика должны вырасти.

```bash
curl -s localhost:8000/metrics | grep '^fraud_scored_total'
curl -s localhost:8001/metrics | grep '^fraud_written_total'
```

Дождитесь данных на графике **Скорость скоринга** и запустите переобучение. DAG при старте на паузе, команда выполняет его один раз. Флаг `publish_demo` нужен, чтобы выпустить демоверсию даже при равном качестве моделей.

```bash
docker compose exec --user airflow airflow airflow dags test retrain_catboost -c '{"publish_demo":true}'
```

После публикации на графике TPS появится оранжевая полоса примерно на 30 секунд. Наведите на неё, чтобы увидеть версию, например `window_20190401`. Скоринг загрузит файл на следующей транзакции. В таблице версий появится новая строка. Счётчик `fraud_model_reloads_total` покажет метку `version` новой модели.

Алерты находятся в [Alerting → Alert rules](http://localhost:3000/alerting/list). Grafana проверяет правила каждые 10 секунд. Условие должно держаться 20 секунд.

- **Поток транзакций остановился**. TPS ниже 0,1 за 20 секунд. Выполните `docker compose stop transaction_simulator`, подождите около 50 секунд и верните поток командой `docker compose start transaction_simulator`.
- **Доля фрода выше 25%**. Проверяется доля за 30 секунд. На демоданных запустите дополнительный поток из строк с `target=1`. Через 40–55 секунд правило должно перейти в Firing. После проверки остановите команду через Ctrl+C.

```bash
docker compose run --rm -e DEMO_FRAUD_ONLY=1 transaction_simulator
```

`docker compose down` останавливает проект и сохраняет данные в томах.
