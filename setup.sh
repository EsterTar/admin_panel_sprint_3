#!/bin/bash
set -e

echo -e "\nCreating file /postgres_to_es/storage.json...\n"
if [ ! -f ./postgres_to_es/storage.json ]; then
    echo "{}" > "./postgres_to_es/storage.json"
else
    echo "\nFile postgres_to_es/storage.json already exists\n."
fi

echo -e "\nBuilding and starting containers...\n"
docker-compose up --detach $(docker-compose config --services | grep -v 'etl')

echo -e "\nCreating tables...\n"
set -a
source .env
set +a
docker-compose exec -it postgres psql -U $DB_USER -d $DB_NAME -a -f /schema_design/movies_database.ddl

echo -e "\nApplying migrations...\n"
docker-compose exec django python manage.py migrate --fake movies 0001_initial
docker-compose exec django python manage.py migrate

echo -e "\nUploading test data to database...\n"
docker-compose exec django python sqlite_to_postgres/load_data.py

echo -e "\nCreating schema in Elasticsearch...\n"
sh ./schema_design/setup-index-elasticsearch.sh

echo -e "\nBuilding and starting ETL container...\n"
docker-compose up --build --detach etl

echo -e "\nSetup completed successfully!"
