CREATE KEYSPACE IF NOT EXISTS stuff WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;

 CREATE TABLE  IF NOT EXISTS stuff.weather (
  uuid UUID PRIMARY KEY,
  temp DOUBLE,
  humd DOUBLE,
  pres DOUBLE,
  device text;
  );