import os
import glob
import psycopg2
from psycopg2 import sql
import pandas as pd
# Disable pandas SettingWithCopyWarning 
pd.options.mode.chained_assignment = None  # default='warn'
from sql_queries import *

def process_json_file(cur, filepath):
    """Read each json file into a pandas dataframe"""
    df_temp = pd.read_json(filepath, lines=True)
    return df_temp
    
def insert_song_data(cur, conn, df, csvpath):
    """
    - Create song and artist DFs from all the song data
    - Create CSV files so we can use COPY for better performance
    - Use COPY to populate the song and artist tables (using query from sql_queries.py)
    """
    # Create song DF from ALL song files
    song_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    # Sort by song title
    song_df = song_df.sort_values('title')

    # Create artist DF from ALL song files
    artist_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    # Sort by artist_name
    artist_df = artist_df.sort_values('artist_name')
    
    # Create CSVs so we can use COPY for better data loading performance    
    song_df.to_csv((csvpath+'/song_df.csv'), index=False)
    artist_df.to_csv((csvpath+'/artist_df.csv'), index=False)
    
    # Insert song data using COPY                            
    copy_path = datapath+'/csv_files/song_df.csv'
    cur.execute(song_table_insert, (copy_path,))
    conn.commit()

    # Insert artist data using COPY
    copy_path = datapath+'/csv_files/artist_df.csv'
    cur.execute(artist_table_insert, (copy_path,))
    conn.commit()
        
def insert_log_data(cur, conn, df, csvpath):
    """
    - Create DF of all log files filtered by NextSong
    - Create time and user DFs and CSV files
    - Use COPY to populate the time and user tables (using queries from sql_queries.py)
    - Loop through each row of the log files to create the songplays table
    """
    # filter by NextSong action
    df = df[df['page'].str.contains("NextSong")]
    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms') 
    df['ts'] = t
    # Sort by start_time
    df = df.sort_values('ts')
    
    # Create time DF from ALL log files
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data, columns = column_labels, dtype = int)
    # Sort by start_time
    time_df = time_df.sort_values('start_time')

    # Create user DF from ALL log files
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.astype({"userId": int})
    
    # Create CSVs so we can use COPY for better data loading performance
    time_df.to_csv((csvpath+'/time_df.csv'), index=False)
    user_df.to_csv((csvpath+'/user_df.csv'), index=False)

    # Insert time data using COPY                            
    copy_path = datapath+'/csv_files/time_df.csv'
    cur.execute(time_table_insert, (copy_path,))
    conn.commit() 

    # Insert user data - loop through all rows instead, based on reviewer comment                     
    for idx, row in enumerate(user_df.itertuples(index=False)):
        songplay_data = (row.userId, row.firstName, row.lastName, row.gender, row.level)        
        cur.execute(user_table_insert, user_data)
        conn.commit() 
        
    # INSERT SONGPLAY DATA: Loop through each row of the log files
    # NOTE: use enumerate as a loop counter for songplay_id
    # Index is False so we get the row number, not the df index
    for idx, row in enumerate(df.itertuples(index=False), start=1):

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, \
                                     row.sessionId, row.location, row.userAgent)   
        cur.execute(songplay_table_insert, songplay_data)    
        conn.commit()
        
def fill_songplay_data(cur, conn, df, csvpath):
    """
    Create a filled songplay table in Postgres with complete artist and song information.
    Use artist_name and song_name from the log data instead of song_id and artist_id.
    """
    df = df[df['page'].str.contains("NextSong")] # Filter to only NextSong actions
    t = pd.to_datetime(df.ts, unit='ms') # convert ts column to datetime
    df['ts'] = t
    # Sort by start_time
    df = df.sort_values('ts')   
    # Select columns (using artist NAME and song NAME instead of ids)
    songplay_df = df[['ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']]
    # Insert songplay_id column (each row is a different songplay_id)
    songplay_df['songplay_id'] = songplay_df.reset_index().index
    songplay_df = songplay_df[['songplay_id', 'ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', \
                               'userAgent']]
    # Create CSV so we can use COPY for better data loading performance
    songplay_df.to_csv(csvpath+'/songplay_df.csv', index=False)
    # Insert data using COPY                            
    copy_path = datapath+'/csv_files/songplay_df.csv'
    cur.execute(songplay_table_insert_2, (copy_path,))
    conn.commit() 
    return songplay_df
        
def quality_check_data(cur, conn, df, idcol, tablename, reset_query):
    """
    Check if the number of table rows in Postgres equals the number of unique ids 
    from the json data.
    If check fails: reset the table in Postgres
    """
    table_rows = cur.execute(sql.SQL('SELECT COUNT(*) FROM {}').format(sql.Identifier(tablename)))
    table_rows = cur.fetchone()
    print('{} total rows in {} table in PostGres.'.format(table_rows[0], tablename))
    if tablename == "songplays":
        num_ids = len(df) # each row is a different songplay_id in the songplays table
    else:
        num_ids = len(df[idcol].unique())
    print('{} total unique {}s from the json files'.format(num_ids, idcol))
    if table_rows[0] != num_ids:
        cur.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(sql.Identifier(tablename)))
        cur.execute(reset_query)
        conn.commit()
        raise ValueError("{} table: Table length and unique ids do not match. Resetting table in PostGres" \
        .format(tablename))
    else:
        print("Check passed!")
        
def process_data(cur, conn, filepath, func):
    """
    - get list of all json files in the directory
    - Iterate and append each json file into the same pandas DF
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
    df = pd.DataFrame()
    for i, datafile in enumerate(all_files, 1):
        df_temp = func(cur, datafile)
        df = df.append(df_temp)
        
    print('{}/{} total files processed.'.format(i, num_files))
    # Return complete df with data from all files    
    return df

def main():
    """Please see the in-line comments for descriptions of what is happening"""
    conn = psycopg2.connect(dbstring) # dbstring defined at top of sql_queries
    cur = conn.cursor()
    
    # Create df of all song data
    df = process_data(cur, conn, filepath='data/song_data', func=process_json_file)
    # Create song and artist dfs and csv files, then copy the data into the Postgres tables
    insert_song_data(cur, conn, df, csvpath='data/csv_files')

    # Quality check song and artist tables: Ensure the number of table rows in Postgres
    # equals the number of unique song and artist ids from the song data
    quality_check_data(cur, conn, df, idcol="song_id", tablename="songs", reset_query=song_table_create)
    quality_check_data(cur, conn, df, idcol="artist_id", tablename="artists", reset_query=artist_table_create)
    
    # Create df of all the log data
    df = process_data(cur, conn, filepath='data/log_data', func=process_json_file)
    # Create time and user dfs and csv files, then copy into Postgres tables
    # Then loop through all NextSong events and insert records into the songplay table
    insert_log_data(cur, conn, df, csvpath='data/csv_files')
    
    # Quality check time, user, and songplay tables: Ensure the number of table rows in Postgres
    # equals the number of unique ids from the log data
    df = df[df['page'].str.contains("NextSong")] # Filter to only NextSong actions
    df = df.astype({"userId": int}) # Set userId to all integers
    quality_check_data(cur, conn, df, idcol="ts", tablename = "time", reset_query=time_table_create)
    quality_check_data(cur, conn, df, idcol="userId", tablename = "users", reset_query=user_table_create)
    quality_check_data(cur, conn, df, idcol="songplay_id", tablename = "songplays", reset_query=songplay_table_create)
    
    # Create a filled songplay table in Postgres with complete artist and song information
    # Use artist_name and song_name from the log data instead of the ids from the song data
    # This is because there is only ONE row in the songplays table with NON NULL song_id and artist ids   
    songplay_df = fill_songplay_data(cur, conn, df, csvpath='data/csv_files')
    # Quality check filled songplays table: Ensure the number of table rows in Postgres
    # equals the number of unique songplay ids from the log data
    quality_check_data(cur, conn, songplay_df, idcol="songplay_id", tablename = "songplays_fill", \
                       reset_query=songplay_table_create_2)

    conn.close()

if __name__ == "__main__":
    main()