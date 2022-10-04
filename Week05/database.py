#!/usr/bin/env python3

import re
import os
import json
import getpass
import datetime
import subprocess
import mysql.connector

# connect to database with user credentials
def connect(hostname, db):
    usernameInput = input('Enter username: ')
    passwordInput = getpass.getpass('Enter password: ')
    connection = mysql.connector.connect(user=usernameInput, password=passwordInput, host=hostname, database=db)
    return connection

# execute query
def query(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)

    tableName = re.search('select .* from ([^ ]*)', query, re.IGNORECASE).group(1)
    queryFields = [i[0] for i in cursor.description]
    queryResults = cursor.fetchall()
    numFields = len(queryFields)

    return {tableName : [dict([(queryFields[i], results[i].strftime("%m_%d_%Y") if isinstance(results[i], datetime.datetime) else results[i]) for i in range(numFields)]) for results in queryResults]}

# get table columns
def describe(table, connection):
    columns = []
    cursor = connection.cursor()
    cursor.execute(f"describe {table}")

    for t in cursor.fetchall():
        columns.append(t[0])
    return columns

# get replica data for each query result
def filter_replicas(results, replicaFields):
    replicas = {'files' : []}
    seenFiles = {}  # fileid : position in list of files

    for result in results["faces_still"]:
        fileid = result["fileid"]
        if fileid not in seenFiles:  # if fileid not yet seen, add new key to replicas
            seenFiles[fileid] = len(seenFiles)
            replicas['files'].append({'fileid' : fileid, 'replicas' : []})
        replicas['files'][seenFiles[fileid]]['replicas'].append({field:result[field] for field in replicaFields})

    return replicas

# get file data for each query result
def select_files(results, fileFields):
    files = {'files' : []}
    seenFiles = set()

    for result in results["faces_still"]:
        fileid = result["fileid"]
        if fileid not in seenFiles:  # if fileid not yet seen, add new key to files
            seenFiles.add(fileid)
            files['files'].append({field:result[field] for field in fileFields})

    return files

# store file data in json
def write_file_json(file, dictionary, filename):
    if not os.path.exists(file):
        with open(file, 'w') as newfile:
            newfile.write(json.dumps({}, indent=4))

    # add entry to json
    with open(file, 'r') as results:
        data = json.load(results)
        data[filename] = dictionary

    with open(file, 'w') as results:
        results.write(json.dumps(data, indent=4))


