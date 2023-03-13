kafka-topics --create --topic weather --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
kafka-console-consumer --topic weather --bootstrap-server localhost:9092