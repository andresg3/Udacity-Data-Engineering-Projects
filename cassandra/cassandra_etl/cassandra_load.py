import logging
import pandas as pd
import etl_util as etl
from sql_queries import *


# function to drop any existing tables and data
def drop_tables(session):
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            logging.error("exception occurred", exc_info=True)

    logging.info('Tables successfully dropped')

# function to re-create tables
def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            logging.error("exception occurred", exc_info=True)
    logging.info('Tables successfully created')

# function to load dataframe into cassandra keyspace
def load_session_item_table(df, session):
    try:
        prepared_session_item_insert = session.prepare(music_app_history_session_item_insert)

        for i, row in df.iterrows():
            session.execute(prepared_session_item_insert,
                            (row['sessionId']
                             , row['itemInSession']
                             , row['artist']
                             , row['song']
                             , row['length'])
                            )


    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

# function to load dataframe into cassandra keyspace
def load_user_session_table(df, session):
    try:
        prepared_user_session_insert = session.prepare(music_app_history_user_session_insert)
        for index, row in df.iterrows():
            session.execute(
                prepared_user_session_insert,
                (
                    row['userId'],
                    row['sessionId'],
                    row['itemInSession'],
                    row['artist'],
                    row['firstName'],
                    row['lastName'],
                    row['song']
                )
            )

    except Exception as e:
        logging.error("exception occurred", exc_info=True)

# function to load dataframe into cassandra keyspace
def load_song_table(df, session):
    try:
        prepared_song_insert = session.prepare(music_app_history_song_insert)
        for index, row in df.iterrows():
            session.execute(
                prepared_song_insert,
                (
                    row['song'],
                    row['userId'],
                    row['firstName'],
                    row['lastName']
                )
            )
    except Exception as e:
        logging.error("exception occurred", exc_info=True)


def main():
    try:

        logging.info('Connecting to Cassandra')
        session, cluster = etl.cassandra_connection()

        drop_tables(session)
        create_tables(session)

        df = pd.read_csv("event_datafile_new.csv")
        logging.info('CSV file successfully loaded')

        load_session_item_table(df, session)
        load_user_session_table(df, session)
        load_song_table(df, session)


    except Exception as e:
        logging.error("exception occurred", exc_info=True)

    finally:
        logging.info('Closing connection to Cassandra')
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
