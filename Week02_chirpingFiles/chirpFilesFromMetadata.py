#!/usr/bin/env python3

import re
import os
import json
import mysql.connector

def MySQL_connect(hostname, db):
    usernameInput = input('Enter username: ')
    passwordInput = input('Enter password: ')
    connection = mysql.connector.connect(user=usernameInput, password=passwordInput, host=hostname, database=db)
    return connection

def MySQL_query(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)

    tableName = re.search('select .* from ([^ ]*) ', query, re.IGNORECASE).group(1)
    queryFields = [i[0] for i in cursor.description]
    queryResults = cursor.fetchall()
    numColumns = len(queryFields)

    resultsDictionary = {tableName : [dict([(queryFields[i], results[i]) for i in range(numColumns)]) for results in queryResults]}

    return resultsDictionary

def main():
    cwd = os.getcwd()
    targetIds = ['235221', '235222', '235220', '235217', '235218', '235219', '235216', '235215', '235214', '235213']
    targetIdsResults = {}

    connection = MySQL_connect('ccldb.crc.nd.edu', 'biometrics')

    # query files and replicas tables for each id in targetIds
    for fileid in targetIds:
        if not os.path.exists(cwd + '/chirpedFiles'):
            os.makedirs(cwd + '/chirpedFiles')

        filesResults = MySQL_query(f'SELECT * from files where fileid={fileid}', connection)
        replicasResults = MySQL_query(f'SELECT * from replicas where fileid={fileid}', connection)
        targetIdsResults[fileid] = {**filesResults, **replicasResults}

        print(f'chirping {replicasResults["replicas"][1]["path"]} from {replicasResults["replicas"][1]["host"]}')
        os.system(f'/afs/crc.nd.edu/group/ccl/software/x86_64/redhat8/cctools/current/bin/chirp {replicasResults["replicas"][1]["host"]} get {replicasResults["replicas"][1]["path"]} chirpedFiles/{filesResults["files"][0]["fileid"]}_{replicasResults["replicas"][1]["replicaid"]}.{filesResults["files"][0]["extension"]}')

        #verify file with md5sum
        md5Result = os.popen(f'md5sum chirpedFiles/{replicasResults["replicas"][1]["path"].split("/")[-1]}').read().split()[0]
        if md5Result == filesResults["files"][0]["checksum"]:
            print("SUCCESS: matching checksum")

    # save query results in json file
    with open("chirpedFiles.json", "w") as outfile:
        outfile.write(json.dumps(targetIdsResults, indent=4, default=str))

    connection.close()

if __name__ == '__main__':
    main()
