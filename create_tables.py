#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drops each table in redshift cluster.
    :param cur: postgres cursor
    :param conn: postgres connection
    :return: none
    """
    for query in drop_table_queries:
        print("\nexecuting: {}".format(query))
        cur.execute(query)
        conn.commit()
    print("\ntables dropped.")


def create_tables(cur, conn):
    """
    creates each table in redshift cluster.
    :param cur: postgres cursor
    :param conn: postgres connection
    :return: none
    """
    for query in create_table_queries:
        print("\nexecuting: {}".format(query))
        cur.execute(query)
        conn.commit()
    print("\ntables created.")


def create_tables_main():
    """
    - Loads configuration parameters (dwh.cfg)

    - Establishes connection with database.

    - Drops all the tables.

    - Creates all tables needed.

    - Closes the connection.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config.get("CLUSTER", "DWH_HOST"),
        config.get("CLUSTER", "DWH_DB_NAME"),
        config.get("CLUSTER", "DWH_DB_USER"),
        config.get("CLUSTER", "DWH_DB_PASSWORD"),
        config.get("CLUSTER", "DWH_PORT")
    ))

    cur = conn.cursor()

    drop_tables(cur, conn)

    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    create_tables_main()
