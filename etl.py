import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 bucket to staging tables.

    Args:
        cur: The cursor object.
        conn: The connection object.

    Returns:
        None.

    Raises:
        Any exceptions raised by the `cur.execute()` or `conn.commit()` methods.
    """
    for query in copy_table_queries:        
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data staging tables into final fact and dimension tables.

    Args:
        cur: The cursor object.
        conn: The connection object.

    Returns:
        None.

    Raises:
        Any exceptions raised by the `cur.execute()` or `conn.commit()` methods.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
