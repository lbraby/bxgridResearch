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
    try:
        cursor.execute(query)
    except mysql.connector.Error as err:
        print("Error executing query: ", err)
        return 0

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
def filter_replicas(results, tablename, replicaFields):
    replicas = {'files' : []}
    seenFiles = {}  # fileid : position in list of files

    for result in results[tablename]:
        fileid = result["fileid"]
        if fileid not in seenFiles:  # if fileid not yet seen, add new key to replicas
            seenFiles[fileid] = len(seenFiles)
            replicas['files'].append({'fileid' : fileid, 'replicas' : []})
        replicas['files'][seenFiles[fileid]]['replicas'].append({field:result[field] for field in replicaFields if field != 'fileid'})

    return replicas

# get file data for each query result
def filter_files(results, tablename, fileFields):
    files = {'files' : []}
    seenFiles = set()

    for result in results[tablename]:
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



# chirp files into filesystem with metadata at root and leaves
def chirp_files(fileResults, replicaResults, tablename, scheme):
    cwd = os.getcwd()

    for i in range(len(fileResults['files'])):
        fileResult = fileResults['files'][i]
        replicas = replicaResults['files'][i]['replicas']

        filename = str(fileResult['fileid']) + '.' + fileResult['extension']
        dirPath = cwd + '/faces_still/' + '/'.join([fileResult[attribute] for attribute in scheme])
        filePath = dirPath + '/' + filename

        if not os.path.exists(dirPath):
            os.makedirs(dirPath)

        for replica in replicas:
            host = replica['host']
            path = replica['path']
            print(f'chirping {path} from {host}')
            if subprocess.call(f'/afs/crc.nd.edu/group/ccl/software/x86_64/redhat8/cctools/current/bin/chirp {host} get {path} {filePath}', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) == 0:
                md5sum = os.popen(f'md5sum {filePath}').read().split()[0]
                if md5sum == fileResult['checksum']:
                    print("SUCCESS: file chirped with matching checksum\n")
                    write_file_json(dirPath + '/results.json', fileResult, filename)
                    break
                else:
                    print("FAILURE: chirped file does not match checksum")
                    os.remove(filePath)     # remove file with incorrect md5sum
            else:
                print("FAILURE: file chirped unsuccessfully")

