#!/usr/bin/env python3

import re
import os
import json
import getpass
import datetime
import subprocess
import mysql.connector
from tqdm import tqdm

# connect to database with user credentials
def connect(hostname, db):
    bxgridDirectory = os.path.expanduser('~') + "/.bxgrid/"
    credentialsFile = bxgridDirectory + "credentials"
    credentialsExists = os.path.exists(credentialsFile)
    usr = ''
    pwd = ''
    if not credentialsExists:
        usr = input('username: ')
        pwd = getpass.getpass('password: ')
    else:
        with open(credentialsFile, 'r') as credentials:
            usr = credentials.readline().rstrip()
            pwd = credentials.readline().rstrip()
    try:
        connection = mysql.connector.connect(user=usr, password=pwd, host=hostname, database=db)
    except:
        return None
    if not credentialsExists:
        if not os.path.exists(bxgridDirectory): # create ~/.bxgrid/
            os.makedirs(bxgridDirectory)
        with open(credentialsFile, 'w') as credentials:
            credentials.write(f"{usr}\n{pwd}\n")
    return connection

# execute query
def query(query, connection):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
    except mysql.connector.Error as err:
        print("Error executing query: ", err)
        return 1

    queryFields = [i[0] for i in cursor.description]
    queryResults = cursor.fetchall()
    numFields = len(queryFields)

    return [dict([(queryFields[i], results[i].strftime("%m_%d_%Y") if isinstance(results[i], datetime.datetime) or isinstance(results[i], datetime.date) else ("NULL" if results[i] is None else ("EMPTY" if str(results[i]).strip() == "" else results[i]))) for i in range(numFields)]) for results in queryResults]

# get table columns
def describe(table, connection):
    columns = []
    cursor = connection.cursor()
    cursor.execute(f"describe {table}")

    for t in cursor.fetchall():
        columns.append(t[0])
    return columns

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

# chirp files and store metdata in root and leaf json files
# nofiles = False
failedHosts = set()
def chirp_replica(replicas, fileResult, filename, dirPath, filePath, largeMetadata):
    skippedHosts = set()
    # attempt chirp until file successfully chirped
    for replica in replicas:
        host = replica['host']
        path = replica['path']

        if host in failedHosts and host not in skippedHosts:
            skippedHosts.add(host)
            replicas.append(replica) # will attempt to chirp from host if all other hosts fail
            continue

        if subprocess.call(f'/afs/crc.nd.edu/group/ccl/software/x86_64/redhat8/cctools/current/bin/chirp {host} get {path} {filePath}', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) == 0:
            md5sum = os.popen(f'md5sum {filePath}').read().split()[0]

            if md5sum == fileResult['checksum']: # file successfully chirped
                # write metadata to root and leaf json files
                write_metadata(fileResult, filename, dirPath, filePath, largeMetadata, False)
                return 0
            else:
                os.remove(filePath)     # remove file with incorrect md5sum
                print(f"Warning: {filename} from host {host} does not match md5sum in metadata.         removing invalid file from filesystem")

        print(f"Warning: {filename} failed to be retreived from host {host}")
        failedHosts.add(host)

    print(f"Error: {filename} could not be retreived")
    return 1

# write metadata to root and leaf json files
def write_metadata(fileResult, filename, dirPath, filePath, largeMetadata, dryrun = True):
    # store metadata in leaf json
    write_file_json(dirPath + '/metadata_refined.json', fileResult, filename)
    # store metadata in root json
    with open(largeMetadata, 'r') as metadata:
        data = json.load(metadata)
        if not dryrun:
            data['chirped-fileids'].append(fileResult["fileid"])
        data['files'][filename] = {'path': filePath,
                                   'metadata': fileResult}
        if fileResult["subjectid"] not in data['subjects']:
            data['subjects'][fileResult['subjectid']] = {}
    with open(largeMetadata, 'w') as metadata:
        metadata.write(json.dumps(data, indent=4))
    return 0

# write subject metadata for new subjects (empty dictionary)
def write_subjects_info(root, connection):
    subjectids = []
    with open(root + '/metadata.json', 'r') as metadata:
        data = json.load(metadata)
        for subject in data["subjects"]:
            if not data["subjects"][subject]:
                subjectids.append(subject)

        subjects = query("select * from subjects where subjectid in ({})".format(','.join(['"{}"'.format(sub) for sub in subjectids])), connection)
        for subject in tqdm(subjects, total = len(subjects), desc="storing subject info"):
            data["subjects"][subject["subjectid"]] = subject
    with open(root + '/metadata.json', 'w') as metadata:
        metadata.write(json.dumps(data, indent=4))

# return sorted dictionary of replica metadata
def query_replicas(fileids, connection):
    replicas = query("select * from replicas where fileid in ({})".format(','.join(map(str, fileids.keys()))), connection)

    sortedReplicas = {}
    for replica in replicas:
        if replica["fileid"] not in sortedReplicas:
            sortedReplicas[replica["fileid"]] = [replica]
        else:
            sortedReplicas[replica["fileid"]].append(replica)

    return sortedReplicas

# chirp files into filesystem with metadata at root and leaves
def chirp_files(files, root, schema, connection = None, nofiles = False):
    if not nofiles:
        replicas = query_replicas(files, connection)

    for fileid in tqdm(files, total = len(files), desc="chirping files with metadata" if not nofiles else "storing metadata"):
        dirPath = root + '/'.join(files[fileid][attribute] for attribute in schema)
        if not os.path.exists(dirPath): # create directory branch
            os.makedirs(dirPath)
        filename = str(files[fileid]["fileid"]) + '.' + files[fileid]["extension"]
        filePath = dirPath + '/' + filename
        largeMetadata = root + 'metadata.json'

        if not nofiles: # chirp files
            chirp_replica(replicas[fileid], files[fileid], filename, dirPath, filePath, largeMetadata)
        else:
            write_metadata(files[fileid], filename, dirPath, filePath, largeMetadata)

