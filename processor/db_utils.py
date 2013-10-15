#!/usr/bin/env python

"""
A simple series of functions using psycopg2 to connect to, search, and insert rows to a postgresql db

Based on: http://initd.org/psycopg/docs/usage.html
          https://wiki.postgresql.org/wiki/Psycopg2_Tutorial
"""

import psycopg2, psycopg2.extras, sys

def connect (dbname, dbuser, pwd=None):
    """Connect to an existing database and return the connection object"""

    try:
        conn = "dbname='"+dbname+"' user='"+dbuser+"'"
        if pwd is not None:
            conn += " password='"+pwd+"'"
        return psycopg2.connect(conn)
    except psycopg2.DatabaseError, e:
        print >>sys.stderr, 'Error:', e

def search (conn, select_string, select_params={}, close_conn=False):
    """Use the database connection to execute a select query and return all the results"""

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Query the database and obtain data as Python objects
    cur.execute(select_string, select_params)
    results = cur.fetchall()

    # Close communication with the database
    cur.close()
    # leave the connection open unless explicitly
    if close_conn:
        conn.close()

    return results

def insert (conn, table, inserts=[], close_conn=False):
    """Use the connection to insert one or more statements to the database,
    where inserts is a list of dicts { table column : value, ... }"""

    # Open a cursor to perform database operations
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # process each {columns=values} dict
    try:
        cur.executemany("INSERT INTO "+ table +" ("+ ','.join(inserts[0].keys()) +") VALUES ("+','.join(map(lambda x: '%('+x+')s', inserts[0].keys()))+")", inserts)
    except psycopg2.IntegrityError:
        # if we wind up trying to insert a duplicate (violating a
        # table UNIQUE constraint) let it go by committing
        # everything to this point, and continue
        conn.commit()
    except psycopg2.OperationalError, e:
        print >>sys.stderr, 'Error:', e

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cur.close()
    # leave the connection open unless explicitly
    if close_conn:
        conn.close()

