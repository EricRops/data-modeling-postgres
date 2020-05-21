import os
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, dbstring, dbstring_default


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    # NOTE: created project with a local Postgres session.
    # Reviewers please ensure the dbstring in sql_queries is correct
    conn = psycopg2.connect(dbstring_default) # dbstring set in sql_queries.py
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(dbstring) # dbstring set in sql_queries.py
    cur = conn.cursor()
    #cur.execute("SET CLIENT_ENCODING TO 'utf8';")
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Closes the connection. 
    - Create csv folder if doesn't already exist
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("sparkifydb tables created")
    
    cur.close()
    conn.close()
    
    if os.path.isdir('data/csv_files') == False:
        os.mkdir('data/csv_files')
        print("csv_files path created") 
    
if __name__ == "__main__":
    main()