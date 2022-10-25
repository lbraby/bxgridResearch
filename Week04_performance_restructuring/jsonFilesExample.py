#!/usr/bin/env python3

import re
import json
import getpass
import datetime
import subprocess
import mysql.connector

def MySQL_connect(hostname, db):
    usernameInput = input('Enter username: ')
    passwordInput = getpass.getpass('Enter password: ')
    connection = mysql.connector.connect(user=usernameInput, password=passwordInput, host=hostname, database=db)
    return connection

def MySQL_query(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)

    tableName = re.search('select .* from ([^ ]*)', query, re.IGNORECASE).group(1)
    queryFields = [i[0] for i in cursor.description]
    queryResults = cursor.fetchall()
    numFields = len(queryFields)

    return {tableName : [dict([(queryFields[i], results[i].strftime("%m_%d_%Y") if isinstance(results[i], datetime.datetime) else results[i]) for i in range(numFields)]) for results in queryResults]}

def MySQL_get_columns(table, connection):
    columns = []
    cursor = connection.cursor()
    cursor.execute(f"describe {table}")

    for t in cursor.fetchall():
        columns.append(t[0])
    return columns

def select_file_data(results, fileFields):
    files = {'files' : []}
    seenFiles = set()

    for result in results["faces_still"]:
        fileid = result["fileid"]
        if fileid not in seenFiles:  # if fileid not yet seen, add new key to files
            seenFiles.add(fileid)
            files['files'].append({field:result[field] for field in fileFields})

    return files

def main():
    # input
    query = "select * from faces_still left join files using (fileid) left join replicas using (fileid) where date like '%2002-03%' and subjectid='nd1S04261';"
    tableName = 'faces_still'
    connection = MySQL_connect('ccldb.crc.nd.edu', 'biometrics')

    faces_stillFields = MySQL_get_columns('faces_still', connection)
    filesFields = MySQL_get_columns('files', connection)
    fileFields = faces_stillFields + filesFields

    results = select_file_data(MySQL_query(query, connection), fileFields)

    with open("filesExample.json", "w") as outfile:
        outfile.write(json.dumps(results, indent=4, default=str))

    connection.close()

if __name__ == "__main__":
    main()
