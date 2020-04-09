import configparser
from dbconn import connect
from sql_queries import create_table_queries, drop_table_queries
import logging


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Tables dropped')


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Tables created')


def main():
    config = configparser.ConfigParser()
    config.read('/home/andresg3/PycharmProjects/redshift_dw/dwh.cfg')

    conn, cur = connect()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()