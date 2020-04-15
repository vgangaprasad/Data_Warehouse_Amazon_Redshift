import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Summary line
    ------------
    Process song & log files from the S3 buckets and copies it to REDSHIFT cluster,
    
    Parameters
    ----------
    cur -- Database Cursor
    conn -- Database connection information
    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Summary line
    ------------
    Process the staging tables created from S3 buckets and uses the data to insert into the STAR Schema tables
    
    Parameters
    ----------
    cur -- Database Cursor
    conn -- Database connection information
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to database based on the parms from the config file (dwh.cfg)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # calls load_staging_tables function to load the staging tables from S3 Buckets
    load_staging_tables(cur, conn)
    # calls insert_tables function to insert the Facts and Dimension tables designed in Redshift cluster
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()