#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    loads/copies song & log data from S3 into staging tables.
    :param cur: postgres cursor
    :param conn: postgres connection
    :return: none
    """

    for query in copy_table_queries:
        print("\nexecuting: {}".format(query))
        cur.execute(query)
        conn.commit()
    print("\ndata loaded into staging tables.")


def insert_tables(cur, conn):
    """
    inserts data from staging tables into analytics tables (star-schema).
    :param cur: postgres cursor
    :param conn: postgres connection
    :return: none
    """
    for query in insert_table_queries:
        print("\nexecuting: {}".format(query))
        cur.execute(query)
        conn.commit()
    print("\ndata inserted into analytics tables.")


def etl():
    # gets parameters from config file dwh.cfg
    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))

    # connection to redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config.get("CLUSTER", "DWH_HOST"),
        config.get("CLUSTER", "DWH_DB_NAME"),
        config.get("CLUSTER", "DWH_DB_USER"),
        config.get("CLUSTER", "DWH_DB_PASSWORD"),
        config.get("CLUSTER", "DWH_PORT")
    ))
    cur = conn.cursor()

    load_staging_tables(cur, conn)

    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    etl()
