# R&D: Handling dynamic SQL in Scala within text2ql problem

## proof-of-concept для <ссылка на доклад>

## работа с динамическими sql [тут](https://github.com/vsevolod66rus/text2ql-examples/blob/main/text2ql/src/main/scala/text2ql/dao/postgres/QueryManager.scala)

### для запуска укажите креды postgres [тут](https://github.com/vsevolod66rus/text2ql-examples/blob/main/text2ql/src/main/resources/params.conf), при старте произойдет [миграция](https://github.com/vsevolod66rus/text2ql-examples/tree/main/text2ql/src/main/resources/migrations), сваггер будет доступен на http://127.0.0.1:3000/docs/ 
### typeDB-server для старта не нужен - клиент ленивый