#!/usr/bin/env python3

import sqlite3

def main():
    try:
        connection = sqlite3.connect('ccldb.crc.nd.edu')
        cursor = connection.cursor()
        
        query = "select sqlite_version();"
        cursor.execute(query)
        record = cursor.fetchall()
        print("SQLite Version: ", record)
        cursor.close()

    except sqlite3.Error as error:
        print("error connecting to sqlite", error)
    finally:
        if connection:
            connection.close()
            print("connection closed")


if __name__ == '__main__':
    main()
