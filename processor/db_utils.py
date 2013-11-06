#!/usr/bin/env python

"""
A simple series of functions using psycopg2 to connect to, search,
and insert rows to a postgresql db

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
    except psycopg2.Error, db_err:
        print >> sys.stderr, db_err.pgcode, db_err.pgerror

def search (conn, select_string, select_params={}, close_conn=False):
    """Use the database connection to execute a select query and return
    all the results"""

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Query the database and obtain data as Python objects
    try:
        cur.execute(select_string, select_params)
        results = cur.fetchall()
    except psycopg2.Error, db_err:
        print >> sys.stderr, db_err.pgcode, db_err.pgerror
        results = None

    # Close communication with the database
    cur.close()
    # leave the connection open unless explicitly
    if close_conn:
        conn.close()

    return results

def insert (conn, table, columns, inserts, close_conn=False):
    """Use the connection to insert one or more statements to the
    database, where inserts is a list of dicts
    { table column : value, ... }"""

    # Open a cursor to perform database operations
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # create the statement to which insert data will be bound
    # (psycopg does not support the notion of prepared statements,
    #  but we can simulate that here, by establishing the core
    #  insert statement once, for which all key/value pairs map)
    column_bindings = map(lambda x: '%('+x+')s', columns)
    insert_stmt = ' '.join(["INSERT INTO", table,
                            "(", ','.join(columns), ")",
                            "VALUES (", ','.join(column_bindings), ")"])
                                 
    # process each {columns=values} dict
    for insert_dict in inserts:
        try:
            cur.execute(insert_stmt, insert_dict)
            conn.commit()
        except psycopg2.Error, ins_err:
            print >> sys.stderr, ins_err.pgcode, ins_err.pgerror
            conn.rollback()

    # Close communication with the database
    cur.close()
    # leave the connection open unless explicitly
    if close_conn:
        conn.close()

