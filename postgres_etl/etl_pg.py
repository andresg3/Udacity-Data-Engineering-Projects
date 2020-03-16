import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, json_file):
    """
    :param cur: cursor reference
    :param json_file: complete file path for the file to load
    """
    # load file into dataframe
    df = pd.DataFrame([pd.read_json(json_file, typ='series', convert_dates=False)])

    # load df values into separate vars
    for val in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = val

    # insert artist record
    artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = (song_id, title, artist_id, year, duration)
    cur.execute(song_table_insert, song_data)

    print(f"Records inserted for file {json_file}")


def process_log_file(cur, json_file):
    """
    :param cur: cursor reference
    :param json_file: complete file path for the file to load
    """

    # open log file
    df = pd.read_json(json_file, lines=True)

    # filter by NextSong action and convert ts column to datetime64[ms]
    df = df[df['page'] == 'NextSong'].astype({'ts': 'datetime64[ms]'})

    # create time series of ts column
    t = df['ts']

    # create time_data dataframe using series created above
    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]
    time_data = []
    for time in t:
        time_data.append([time, time.hour, time.day, time.weekofyear, time.month, time.year, time.day_name()])

    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    # insert time_data df to time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)

    # create user_df and insert to user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for index, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    # get songid and artistid from song and artist tables
    for i, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

    print(f"Records inserted for file {json_file}")

def process_data(cur, conn, file_path, func):
    """
    Driver function to load data from songs and event log files into Postgres database.
    :param cur: a database cursor reference
    :param conn: database connection reference
    :param file_path: parent directory where the files exists
    :param func: function to call
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(file_path):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, file_path))

    # iterate over files and process
    for i, json_file in enumerate(all_files, 1):
        func(cur, json_file)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Driver function for loading songs and log data into Postgres database
    """
    # create connection to postgres: conn, cur
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
    cur = conn.cursor()

    # call process_data function
    process_data(cur, conn, file_path='data/song_data', func=process_song_file)
    print("Processing of SONG data files completed\n")
    process_data(cur, conn, file_path='data/log_data', func=process_log_file)
    print("Processing of LOG data files completed\n")
    # close connection
    conn.close()


if __name__ == "__main__":
    main()
    print("\n\nFinished processing!!!\n\n")