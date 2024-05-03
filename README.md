# Movies Admin

# Запуск проекта

1) В корне проекта лежит файл `.env.example`. Необходимо создать файл `.env` и указать в нем переменные окружения. Для тестового запуска можно использовать значения из `.env.example`.
2) В директории `/postgres_to_es` необходимо создать файл `storage.json`, если его еще не существует. В ином случае создаcтся директория `storage.json`
3) Из корня проекта выполните файл `setup.sh` - `bash setup.sh`.
   1) Скрипт билдит и запускает контейнеры из `docker-compose.yml` файла.
   2) Создает таблицы в БД из `schemas_design/movies_database.ddl`
   3) Применят миграции в БД.
   4) Загружает тестовые данные из `sqlite_to_postgres/load_data.py`
   5) Создает schema для elasticsearch, если её еще не существует.
4) Создайте `superuser` для админки Django. Для этого выполните `docker-compose exec django python manage.py createsuperuser` и заполните запрашиваемые данные.
5) Админка проекта открывается по url **[http://localhost/admin](http://localhost/admin)**.
6) API проекта открывается по url **http://localhost/api/v1/movies/**, либо **http://localhost:8000/api/v1/movies/**
