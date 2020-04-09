import psycopg2
import configparser
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def connect():
    try:
        config = configparser.ConfigParser()
        config.read('/home/andresg3/PycharmProjects/redshift_dw/dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        logging.info('Connection successful')
        return conn, cur

    except Exception as e:
        logging.error("exception occurred", exc_info=True)