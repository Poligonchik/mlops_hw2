# Real-Time Fraud Detection System

Система потокового обнаружения мошеннических транзакций с использованием CatBoost, Apache Kafka, PostgreSQL, Streamlit и Docker Compose.

## Архитектура

В проект входят следующие сервисы:

1. **`interface` — Streamlit UI**
   - загружает CSV-файлы с транзакциями
   - генерирует уникальный ID для каждой транзакции
   - отправляет транзакции отдельными JSON-сообщениями в топик `transactions`
   - получает результаты из PostgreSQL
   - показывает последние фродовые транзакции
   - строит гистограмму модельных скоров

2. **`fraud_detector` — ML-сервис**
   - читает транзакции из топика `transactions`
   - выполняет препроцессинг
   - применяет обученную CatBoost-модель
   - формирует `score` и `fraud_flag`
   - отправляет результат в топик `scores`

3. **`results_consumer` — сервис сохранения результатов**
   - читает сообщения из топика `scores`
   - сохраняет `transaction_id`, `score` и `fraud_flag` в PostgreSQL
   - фиксирует Kafka offset только после успешной записи результата в базу

4. **`postgres` — база данных**
   - хранит результаты скоринга в таблице `transaction_scores`
   - автоматически создаёт таблицу и индексы при первом запуске

5. **Kafka Infrastructure**
   - `zookeeper` — координация Kafka;
   - `kafka` — брокер сообщений;
   - `kafka-setup` — автоматическое создание топиков `transactions` и `scores`
   - `kafka-ui` — веб-интерфейс для просмотра топиков и сообщений

## Схема обработки данных

```text
CSV-файл
    ↓
Streamlit UI
    ↓
Kafka: transactions
    ↓
Препроцессинг
    ↓
CatBoost inference
    ↓
Kafka: scores
    ↓
results_consumer
    ↓
PostgreSQL
    ↓
Streamlit: таблица и гистограмма
```

## ML-модель

В проекте используется `CatBoostClassifier`.

Модель была предварительно обучена вне Docker-контейнера и сохранена в файле:
```text
fraud_detector/models/my_model.cbm
```
Настройки модели и список используемых признаков находятся в файле:
```text
fraud_detector/models/my_model_metadata.json
```
ROC-AUC на валидационной выборке:
```text
0.9957
```

## Препроцессинг

Препроцессинг реализован в отдельном файле:

```text
fraud_detector/src/features.py
```

- преобразование `transaction_time` в час, день недели и месяц
- расчёт расстояния между координатами клиента и магазина
- обработку пропущенных категориальных значений
- преобразование категориальных признаков в строковый формат для CatBoost
- удаление полей `name_1`, `name_2` и `street`

## Структура проекта

```text
.
├── fraud_detector/
│   ├── app/
│   │   └── app.py
│   ├── models/
│   │   ├── my_model.cbm
│   │   └── my_model_metadata.json
│   ├── src/
│   │   ├── features.py
│   │   └── scorer.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .dockerignore
├── interface/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── postgres/
│   └── init.sql
├── results_consumer/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── training/
│   └── train_model.py
├── docker-compose.yaml
├── .gitignore
└── README.md
```

## Быстрый старт
### Требования

- Docker Desktop или Docker Engine 20.10+
- Docker Compose V2

### 1. Клонирование репозитория

### 2. Сборка и запуск контейнеров

```bash
docker compose up --build -d
```

## Использование

### Streamlit UI

После запуска:

[http://localhost:8501](http://localhost:8501)


Порядок работы:

1. Открыть Streamlit UI.
2. Загрузить CSV-файл формата `test.csv`.
3. Нажать кнопку «Отправить».
4. Дождаться завершения отправки.
5. Нажать кнопку «Посмотреть результаты».

Рекомендую загрузить небольшой сэмпл 100 транзакций https://disk.360.yandex.ru/d/Rg0dEqJxYXipTA

Полный test.csv загрузить из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025

Можно из него сделать маленький сэмпл командой:

```bash
python -c "import pandas as pd; pd.read_csv('test.csv').head(100).to_csv('test_small.csv', index=False)"
```

### Раздел результатов

После нажатия кнопки «Посмотреть результаты» интерфейс выводит:

1. 10 последних транзакций из PostgreSQL с `fraud_flag = 1`, если такие транзакции есть.
2. Гистограмму распределения скоров последних 100 транзакций.

Если в базе меньше 100 транзакций, гистограмма строится по всем имеющимся записям.

### Kafka UI

Kafka UI доступен по адресу:

[http://localhost:8080](http://localhost:8080)

В интерфейсе можно посмотреть:

- входные сообщения в топике `transactions`
- результаты скоринга в топике `scores`
- partitions, offsets и consumer groups
