ksqldb-cli:
	cd ksqldb && docker-compose up -d && ./ksql-cli.sh

init-mysql-full:
	cat mysql-full/create.sql mysql-full/alter.sql > db/dump/init.sql
	cd mysql-full && docker-compose up -d && sleep 15

init-kafka:
	cd kafka && docker-compose up -d

init-debezium:
	cat debezium/create.sql debezium/alter.sql > db/dump/init.sql
	cd debezium && docker-compose up -d && sleep 20
	curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium/register-mysql.json

init-debezium-kstreams:
	cat debezium-kstreams/create.sql debezium-kstreams/alter.sql > db/dump/init.sql
	cd debezium-kstreams && docker-compose up -d && sleep 20
	curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium-kstreams/register-mysql.json

init-ksqldb-debezium:
	cat debezium/create.sql debezium/alter.sql > db/dump/init.sql
	cd ksqldb-debezium && docker-compose up -d && sleep 20
	curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium/register-mysql.json

generate_data: fake_data/airports.csv fake_data/clients.csv fake_data/aircraft_brands.txt
	sbt "runMain ch.hepia.DataGenerator $^"

fake_data/clients.csv:
	for i in {1..10}; do curl "https://api.mockaroo.com/api/a836c3c0?count=1000&key=111c18a0" >> $@; done

clean:
	rm -rf .bloop .metals target project/project project/target project/.bloop
