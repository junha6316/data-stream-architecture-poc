### 명령어 정리

## cassandra

cqlsh <host> <port> : 카산드라 shell 접근 없으면 brew install cassandra

USE your_keyspace;
DESCRIBE TABLE your_table;

CREATE KEYSPACE stuff
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
AND durable_writes = true;

CREATE TABLE stuff.weather (
uuid UUID PRIMARY KEY,
temp DOUBLE,
humd DOUBLE,
pres DOUBLE,

);

### 에러

List directory /var/lib/apt/lists/partial is missing. - Acquire (2: No such file or directory)
