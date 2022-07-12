import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
    takes in the cursor and the filepath,
    opens the file, uses the cursor to insert the songs data into the database
    """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[["song_id","title","artist_id","duration"]].values.tolist()
    song_data = song_data[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()
    artist_data = artist_data[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    takes in the cursor and the filepath,
    opens the file, uses the cursor to insert the logs data into the database
    """
    # open log file
    df =  pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # converting ts from milliseconds to seconds first
    df["ts"] = df["ts"] / 1000.0
    # converting ts to timestamp
    df["ts"] = df["ts"].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    # converting ts to datetime
    df['ts'] = pd.to_datetime(df['ts'])
    # extract timestamp
    #df['timestamp'] = df['ts'].dt.timestamp
    # extract hour
    df['hour'] = df['ts'].dt.hour
    # extract day
    df['day'] = df['ts'].dt.day
    # extract week
    df['week'] = df['ts'].dt.week
    # extract month
    df['month'] = df['ts'].dt.month
    # extract year
    df['year'] = df['ts'].dt.year
    # extract weekday
    df['weekday'] = df['ts'].dt.weekday
    # creating time_df
    time_df = df[["ts", "hour", "day", "week", "month", "year", "weekday"]]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    #user_df = user_df.drop_duplicates()


    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    takes in a cursor, connection string, filepath, and the function
    iterates all over the ".json" files in a directory and run either
    process_song_file or process_log_file on them file by file.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()