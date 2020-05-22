# Set strings for database connections
#dbstring = "host=localhost dbname=sparkifydb user=ericr password=test123"
#dbstring_default = "host=localhost dbname=postgres user=ericr password=test123"
dbstring = "host=127.0.0.1 dbname=sparkifydb user=student password=student" # for Udacity server
dbstring_default = "host=127.0.0.1 dbname=studentdb user=student password=student" # for Udacity server

# Set data paths for efficient loading using COPY
#datapath = r'C:/Users/ericr/Google Drive/Online Courses/Udacity/Data Engineering/Part 1 - Data Modeling/data-modeling-postgres/data'
datapath = "/home/workspace/data" # for Udacity server

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
songplay_table_drop_2 = "DROP TABLE IF EXISTS songplays_fill"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# Using PRIMARY KEY prevent duplicate rows
# Using NOT NULL to prevent null values on relevant columns 
# (some columns have nulls, such as song_id, artist_id, location, latitude)

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays (songplay_id int PRIMARY KEY, \
                                                                start_time timestamp NOT NULL, \
                                                                user_id int NOT NULL, \
                                                                level varchar NOT NULL, \
                                                                song_id varchar, \
                                                                artist_id varchar, \
                                                                session_id int NOT NULL, \
                                                                location varchar, \
                                                                user_agent varchar NOT NULL);")

songplay_table_create_2 = ("CREATE TABLE IF NOT EXISTS songplays_fill (songplay_id int PRIMARY KEY, \
                                                                start_time timestamp NOT NULL, \
                                                                user_id int NOT NULL, \
                                                                level varchar NOT NULL, \
                                                                song_name varchar, \
                                                                artist_name varchar, \
                                                                session_id int NOT NULL, \
                                                                location varchar, \
                                                                user_agent varchar NOT NULL);")

user_table_create = ("CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, \
                                                        first_name varchar NOT NULL, \
                                                        last_name varchar NOT NULL, \
                                                        gender varchar NOT NULL, \
                                                        level varchar NOT NULL);")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, \
                                                        title varchar NOT NULL, \
                                                        artist_id varchar NOT NULL, \
                                                        year int NOT NULL, \
                                                        duration numeric NOT NULL);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, \
                                                            name varchar NOT NULL, \
                                                            location varchar, \
                                                            latitude numeric, \
                                                            longitude numeric);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, \
                                                       hour int NOT NULL, \
                                                       day int NOT NULL, \
                                                       week int NOT NULL, \
                                                       month int NOT NULL, \
                                                       year int NOT NULL, \
                                                       weekday int NOT NULL);")
# INSERT RECORDS
# NOTE: using INSERT INTO for the songplays table because I feel it makes more sense 
# to loop through the log file df ONLY BECAUSE of the join required for each NextSong event.
# Please let me know if there is a more efficient way to do this

songplay_table_insert = ("INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, \
                                                 location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
                                                 ON CONFLICT (songplay_id) DO NOTHING;")
# Procedure for COPY:
# First, create a temp table, and COPY the entire CSV file into the temp table
# Then, use INSERT to transfer all data from the temp table to the final table (songs, users, time, artists, or songplays)
# Finally, ON CONFLICT (PRIMARY_KEY) DO NOTHING to ensure the transactions skip over duplicate primary keys!

# For users table, need to first delete rows with duplicate user ids keeping the most recent row 
# so the DO UPDATE to track changing user levels does not crash.
# Note users is NOT sorted by user_id so we can preserve the latest level status
user_table_insert = ("CREATE TEMP TABLE tmp_table ON COMMIT DROP \
                      AS \
                      SELECT * FROM users WITH NO DATA;" \
                     "COPY tmp_table FROM %s DELIMITERS ',' CSV HEADER;" \
                     "DELETE FROM tmp_table T1 \
                          USING tmp_table T2 \
                          WHERE T1.ctid < T2.ctid \
                          AND T1.user_id = T2.user_id;" \
                     "INSERT INTO users \
                     SELECT * FROM tmp_table \
                     ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;")

song_table_insert = ("CREATE TEMP TABLE tmp_table ON COMMIT DROP \
                      AS \
                      SELECT * FROM songs WITH NO DATA;" \
                     "COPY tmp_table FROM %s DELIMITERS ',' CSV HEADER;" \
                     "INSERT INTO songs \
                     SELECT * FROM tmp_table \
                     ON CONFLICT (song_id) DO NOTHING;")

artist_table_insert = ("CREATE TEMP TABLE tmp_table ON COMMIT DROP \
                      AS \
                      SELECT * FROM artists WITH NO DATA;" \
                     "COPY tmp_table FROM %s DELIMITERS ',' CSV HEADER;" \
                     "INSERT INTO artists \
                     SELECT * FROM tmp_table \
                     ON CONFLICT (artist_id) DO NOTHING;")

time_table_insert = ("CREATE TEMP TABLE tmp_table ON COMMIT DROP \
                      AS \
                      SELECT * FROM time WITH NO DATA;" \
                     "COPY tmp_table FROM %s DELIMITERS ',' CSV HEADER;" \
                     "INSERT INTO time \
                     SELECT * FROM tmp_table \
                     ON CONFLICT (start_time) DO NOTHING;")

songplay_table_insert_2 = ("CREATE TEMP TABLE tmp_table ON COMMIT DROP \
                      AS \
                      SELECT * FROM songplays_fill WITH NO DATA;" \
                     "COPY tmp_table FROM %s DELIMITERS ',' CSV HEADER;" \
                     "INSERT INTO songplays_fill \
                     SELECT * FROM tmp_table \
                     ON CONFLICT (songplay_id) DO NOTHING;")

# FIND SONGS

song_select = ("SELECT s.song_id, a.artist_id \
                FROM songs s JOIN artists a \
                ON s.artist_id = a.artist_id \
                WHERE s.title = %s AND a.name = %s AND s.duration = %s;")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, \
                       songplay_table_create_2]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, \
                     songplay_table_drop_2]