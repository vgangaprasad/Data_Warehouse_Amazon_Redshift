import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


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
    # Read the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to database based on the parms from the config file (dwh.cfg)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Calls Drop tables function to drop the tables before creating them.
    drop_tables(cur, conn)
    
    # Calls create_tables function to create the tables.
    create_tables(cur, conn)
    
    # close connection to default database
    conn.close()


if __name__ == "__main__":
    main()