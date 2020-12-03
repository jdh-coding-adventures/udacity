import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = get_files(filepath)
    song_files = df[0]
    files_df = pd.read_json(song_files,line=True)

    # insert song record
    song_data = files_df[["song_id","title","artist_id","year","duration"]].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = files_df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = get_files(filepath)
    log_path = df[0]
    df = pd.read_json(log_path,lines=True)

    # filter by NextSong action
    df = df[df.page == "Next Song"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"],unit="ms")
    
    # insert time data records
    time_data = []
    for item in t:
        time_data.append([item, item.hour, item.day, item.week, item.month, item.year ,item.dayofweek])
        
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(data=time_data,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    users = get_files("data/log_data")
    filepath = users[0]
    users_data = pd.read_json(filepath,lines=True)
    user_df = users_data[["userId","firstName","lastName","gender","level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    
    
    files = get_files("data/log_data")
    filepath = files[0]

    raw_df = pd.read_json(filepath,lines=True)
    df2 = raw_df[["ts","userId","level","sessionId","location","userAgent","song","artist","length"]]

    new_lst = []
    for item,row in df2.iterrows():
        new_lst.append([pd.to_datetime(row.ts,unit='ms'),row.userId,row.level,row.sessionId,row.location,row.userAgent,row.song,row.artist,row.length])


    column_labels = ["timestamp","user_id","level","session_id","location","user_agent","song","artist","length"]
    df = pd.DataFrame(new_lst,columns=column_labels)
    
    
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (results)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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