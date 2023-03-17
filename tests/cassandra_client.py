# cassandra access test script

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


def main():
    # Connect to the Cassandra cluster
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # Create a new keyspace
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    )

    # Use the new keyspace
    session.set_keyspace("test_keyspace")

    # Create a new table
    session.execute(
        "CREATE TABLE IF NOT EXISTS test_table (id uuid PRIMARY KEY, name text, age int);"
    )

    # Insert data into the table
    insert_data = SimpleStatement(
        "INSERT INTO test_table (id, name, age) VALUES (uuid(), %s, %s)"
    )
    session.execute(insert_data, ("Alice", 30))
    session.execute(insert_data, ("Bob", 28))

    # Query data from the table
    query_data = "SELECT * FROM test_table"
    rows = session.execute(query_data)

    for row in rows:
        print(row)

    # Close the connection
    cluster.shutdown()


if __name__ == "__main__":
    main()
