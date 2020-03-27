import logging
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

def cassandra_connection():
    """
    Connection object for Cassandra
    :return: session, cluster
    """
    try:
        cluster = Cluster(['172.17.0.2'], port=9042)
        session = cluster.connect()

        session.execute("""
                CREATE KEYSPACE IF NOT EXISTS sparkify
                WITH REPLICATION =
                { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
                """)

        session.set_keyspace('sparkify')
        logging.info('Connection successful')

        return session, cluster

    except Exception as e:
        logging.error("exception occurred", exc_info=True)

if __name__ == "__main__":
    logging.info('Not callable')