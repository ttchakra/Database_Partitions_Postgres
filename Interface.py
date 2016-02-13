#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

RATINGS_TABLE_NAME = 'Ratings'


def getopenconnection(user='postgres', password='__PASSWORD_FOR_USER_POSTGRES__', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + +user + "' host='localhost' password='" + password + "'")


def loadratings(ratingsfilepath):
    pass


def rangepartition(ratingstablename, numberofpartitions):
    pass


def roundrobinpartition(ratingstablename, numberofpartitions):
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating):
    pass


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = psycopg2.connect(user='postgres', host='localhost', password='__PASSWORD_FOR_USER_POSTGRES__')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


if __name__ == '__main__':
    try:
        create_db('dds_assgn1')
    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
