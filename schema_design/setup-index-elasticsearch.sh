#!/bin/bash
set -e

until curl -s http://127.0.0.1:9200/; do
    echo "Elasticsearch not available, wait..."
    sleep 1
done

if curl -s --head --fail "http://localhost:9200/movies" | grep -q "200 OK"; then
    echo "Index already exists. Skip."
else
  curl -X PUT "http://localhost:9200/movies" -H 'Content-Type: application/json' -d'
  {
    "settings": {
      "refresh_interval": "1s",
      "analysis": {
        "filter": {
          "english_stop": {
            "type": "stop",
            "stopwords": "_english_"
          },
          "english_stemmer": {
            "type": "stemmer",
            "language": "english"
          },
          "english_possessive_stemmer": {
            "type": "stemmer",
            "language": "possessive_english"
          },
          "russian_stop": {
            "type": "stop",
            "stopwords": "_russian_"
          },
          "russian_stemmer": {
            "type": "stemmer",
            "language": "russian"
          }
        },
        "analyzer": {
          "ru_en": {
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "english_stop",
              "english_stemmer",
              "english_possessive_stemmer",
              "russian_stop",
              "russian_stemmer"
            ]
          }
        }
      }
    },
    "mappings": {
      "dynamic": "strict",
      "properties": {
        "id": { "type": "keyword" },
        "imdb_rating": { "type": "float" },
        "genres": { "type": "keyword" },
        "title": {
          "type": "text",
          "analyzer": "ru_en",
          "fields": { "raw": { "type": "keyword" } }
        },
        "description": { "type": "text", "analyzer": "ru_en" },
        "directors_names": { "type": "text", "analyzer": "ru_en" },
        "actors_names": { "type": "text", "analyzer": "ru_en" },
        "writers_names": { "type": "text", "analyzer": "ru_en" },
        "directors": { "type": "nested", "dynamic": "strict", "properties": { "id": { "type": "keyword" }, "name": { "type": "text", "analyzer": "ru_en" } } },
        "actors": { "type": "nested", "dynamic": "strict", "properties": { "id": { "type": "keyword" }, "name": { "type": "text", "analyzer": "ru_en" } } },
        "writers": { "type": "nested", "dynamic": "strict", "properties": { "id": { "type": "keyword" }, "name": { "type": "text", "analyzer": "ru_en" } } }
      }
    }
  }
  '
fi