#!/usr/bin/env python3

import re
import os
import json
import getpass
import datetime
import subprocess
import mysql.connector
import time

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

def write_to_json(filepath, dictionary):
    # file does not exist
    if not os.path.exists(filepath):
        with open(filepath, 'w') as newfile:
            newfile.write(json.dumps({'results' : []}, indent=4))

    # append replica entry to json
    fileData = {}
    with open(filepath, 'r') as results:
        fileData = json.load(results)
        fileData['results'].append(dictionary)
    with open(filepath, 'w') as results:
        results.write(json.dumps(fileData, indent=4))

def MySQL_get_columns(table, connection):
    columns = []
    cursor = connection.cursor()
    cursor.execute(f"describe {table}")

    for t in cursor.fetchall():
        columns.append(t[0])
    return columns

# selects replica data from results of MySQL_query
def select_replica_data(results, replicasFields):
    replicas = {'files' : []}
    seenFiles = {}  # fileid : position in list of files

    for result in results["faces_still"]:
        fileid = result["fileid"]
        if fileid not in seenFiles:  # if fileid not yet seen, add new key to replicas
            seenFiles[fileid] = len(seenFiles)
            replicas['files'].append({'fileid' : fileid, 'replicas' : []})
        replicas['files'][seenFiles[fileid]]['replicas'].append({field:result[field] for field in replicasFields})

    return replicas

def select_file_data(results, fileFields, subjectIDs):
    files = {'files' : []}
    seenFiles = set()

    for result in results["faces_still"]:
        fileid = result["fileid"]
        if fileid not in seenFiles:  # if fileid not yet seen, add new key to files
            seenFiles.add(fileid)
            files['files'].append({field:result[field] for field in fileFields})
        subjectID = result["subjectid"]
        if subjectID not in subjectIDs:
            subjectIDs.append(subjectID)

    return files

def chirp_files_2(fileResults, replicaResults, namingParameters, branchingScheme):
    cwd = os.getcwd()

    for i in range(len(fileResults['files'])):
        fileResult = fileResults['files'][i]
        replicas = replicaResults['files'][i]['replicas']

        filename = '_'.join([fileResult[parameter] for parameter in namingParameters]) + '.' + fileResult['extension']
        dirPath = cwd + '/faces_still/' + '/'.join([fileResult[branch] for branch in branchingScheme])
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

def write_file_json(jsonFile, newData, filename):
    if not os.path.exists(jsonFile):
        with open(jsonFile, 'w') as newfile:
            newfile.write(json.dumps({}, indent=4))

    # add entry to json
    with open(jsonFile, 'r') as results:
        data = json.load(results)
        data[filename] = newData

    with open(jsonFile, 'w') as results:
        results.write(json.dumps(data, indent=4))


# create subjectinfo.json in subject id branches
def write_subject_json(subjectResults):
    cwd = os.getcwd()

    for subject in subjectResults['subjects']:
        subjectid = subject['subjectid']
        filepath = cwd + '/faces_still/' + subjectid + '/subjectinfo.json'
        with open(filepath, 'w') as subjectjson:
            subjectjson.write(json.dumps(subject, indent=4, default=str))

def main():
    # input
    limit = int(input("rumber of entries: "))
    query = f"select * from faces_still left join files using (fileid) left join replicas using (fileid) order by fileid limit {limit};"
    tableName = 'faces_still'
    connection = MySQL_connect('ccldb.crc.nd.edu', 'biometrics')

    # starting time
    start = time.time()

    # main query
    results = MySQL_query(query, connection)
    subjectIDs = []

    # capture replica and file results in seperate dictionaries
    replicaFields = [field for field in MySQL_get_columns('replicas', connection) if field not in ['fileid']]
    replicaResults = select_replica_data(results, replicaFields)

    fileFields = MySQL_get_columns('faces_still', connection) + MySQL_get_columns('files', connection)
    fileResults = select_file_data(results, fileFields, subjectIDs)

    # query data for each subject with file in results
    subjects = '(' + ', '.join(['"' + ID + '"' for ID in subjectIDs]) + ')'
    query = f"select * from subjects where subjectid in {subjects}"
    subjectResults = MySQL_query(query, connection)

    # chirp files, placing metadata for subjectids and files in json files
    chirp_files_2(fileResults, replicaResults, ['recordingid', 'sequenceid'], ['subjectid', 'date'])

    write_subject_json(subjectResults)

    connection.close()

    # ending time
    end = time.time()
    elapsed = end - start

    with open("testResults.txt", "a") as timeFile:
        timeFile.write(f"limit {limit} - {elapsed:.2f} sec\n")

if __name__ == "__main__":
    main()
