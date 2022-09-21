#!/usr/bin/env python3

import re
import os
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

def filter_results(results, tablename, key):
    filteredResults = {}

    # place results in lists separated by keys
    for entry in results[tablename]:
        entryKey = entry[key]

        if entryKey not in filteredResults:
            filteredResults[entryKey] = []
        filteredResults[entryKey].append(entry)

    return filteredResults

def chirp_files(filteredResults, tablename, branchKeys, namingParameters):
    cwd = os.getcwd()

    for replicaEntries in filteredResults.values():
        # establish file naming conventions and file system directory branching
        filename = '_'.join([replicaEntries[0][parameter] for parameter in namingParameters]) + '.' + replicaEntries[0]['extension']
        dirPath = cwd + '/' + tablename +  '/' + '/'.join([replicaEntries[0][key] for key in branchKeys])
        fullFilePath = dirPath + '/' + filename

        if not os.path.exists(dirPath):
            os.makedirs(dirPath)

        # chirp file from replica hosts till success
        for entry in replicaEntries:
            host = entry['host']
            path = entry['path']
            if subprocess.call(f'/afs/crc.nd.edu/group/ccl/software/x86_64/redhat8/cctools/current/bin/chirp {host} get {path} {fullFilePath}', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) == 0:
                print(f'chirping {path} from {host}')
                md5sum = os.popen(f'md5sum {fullFilePath}').read().split()[0]
                if md5sum == entry['checksum']:
                    print("SUCCESS: file chirped with matching checksum")
                    break
                else:
                    print("FAILURE: chirped file does not match checksum")


def main():
    # input
    query = "select * from faces_still left join files using (fileid) left join replicas using (fileid) where date like '%2002-03%' and subjectid='nd1S04261';"
    tableName = 'faces_still'
    connection = MySQL_connect('ccldb.crc.nd.edu', 'biometrics')

    results = MySQL_query(query, connection)
    results = filter_results(results, tableName, 'fileid')

    chirp_files(results, tableName, ['subjectid', 'date'], ['recordingid','sequenceid'])

    with open("testOutput.json", "w") as outfile:
        outfile.write(json.dumps(results, indent=4, default=str))

    connection.close()

if __name__ == "__main__":
    main()
