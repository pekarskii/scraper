# Car Ads Scraper

Этот проект представляет собой набор скриптов для поиска и парсинга объявлений о продаже автомобилей на различных платформах, таких как [av.by](https://cars.av.by) и [cars.com](https://www.cars.com). Данные сохраняются в базу данных (MySQL или MS SQL) для дальнейшего анализа.

![image](https://github.com/user-attachments/assets/a5253b8b-8d4b-483f-aaa5-07909d52477d)

## Функциональность

Проект включает в себя два основных модуля:
1. **Finder** — поисковик ссылок на объявления с авто-площадок.
2. **Scraper** — сборщик информации с объявлений, найденных с помощью Finder.


![image](https://github.com/user-attachments/assets/6bee24ca-8440-41f4-ad7c-5e780f71a0cd)

Дополнительно есть модуль для симуляции нагрузки на базу данных.

## Структура файлов

- `cards_finder_av_by_mssql.py` - поиск объявлений на av.by, сохранение в MS SQL.
- `cards_finder_cars_com.py` - поиск объявлений на cars.com, сохранение в MySQL.
- `cards_finder_cars_com_mssql.py` - поиск объявлений на cars.com, сохранение в MS SQL.
- `cards_scrapper_av_by_mssql.py` - парсинг данных об автомобилях с av.by, сохранение в MS SQL.
- `cards_scrapper_cars_com.py` - парсинг данных об автомобилях с cars.com, сохранение в MySQL.
- `cards_scrapper_cars_com_mssql.py` - парсинг данных об автомобилях с cars.com, сохранение в MS SQL.
- `simulate_recorded_workload.py` - скрипт для симуляции нагрузки на MySQL на основе логов медленных запросов.

## Установка и запуск

### 1. Установка зависимостей

Перед запуском убедитесь, что у вас установлен Python 3 и необходимые библиотеки:

```bash
pip install -r requirements.txt
```

### 2. Конфигурация

Создайте файл `config.json` в корневой директории и укажите параметры подключения к базам данных:

```json
{
    "mssql_audit_db": {
        "server": "your_server",
        "user": "your_user",
        "password": "your_password",
        "database": "your_db"
    },
    "audit_db": {
        "host": "your_host",
        "user": "your_user",
        "password": "your_password",
        "database": "your_db"
    }
}
```

### 3. Запуск

Для поиска объявлений используйте:
```bash
python cards_finder_av_by_mssql.py
python cards_finder_cars_com.py
```

Для парсинга данных:
```bash
python cards_scrapper_av_by_mssql.py
python cards_scrapper_cars_com.py
```

Для тестирования нагрузки:
```bash
python simulate_recorded_workload.py
```

## Требования
- Python 3.6+
- `requests`, `BeautifulSoup`, `pymysql`, `pymssql`, `json`, `pandas`


![image](https://github.com/user-attachments/assets/b69d0f87-2a9c-4b0e-a835-c59a706124a5)


