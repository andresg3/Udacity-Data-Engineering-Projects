from sql_queries import copy_table_queries, insert_table_queries
import logging
from dbconn import connect


def load_staging_tables(conn, cur):
    logging.info('Loading staging tables')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Completed loading staging tables')


def insert_tables(conn, cur):
    logging.info('Loading target tables')
    for q in insert_table_queries:
        cur.execute(q)
        conn.commit()
    logging.info('Completed loading target tables')


def main():
    try:
        conn, cur = connect()

        load_staging_tables(conn, cur)
        insert_tables(conn, cur)

        conn.close()
        logging.info('Closing connection')

    except Exception as e:
        logging.error("exception occurred", exc_info=True)

if __name__ == "__main__":
    main()