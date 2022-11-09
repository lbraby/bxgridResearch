#!/usr/bin/env python3

import re
import os
import json
import getpass
import datetime
import subprocess
import mysql.connector
from typing import List, Dict
from tqdm import tqdm

# connect to database with user credentials
def connect():
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
        connection = mysql.connector.connect(user=usr, password=pwd, host='ccldb.crc.nd.edu', database='biometrics')
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
        print("Error executing query:", err)
        return None

    queryFields = [i[0] for i in cursor.description]
    queryResults = cursor.fetchall()
    numFields = len(queryFields)

    results = [queryFields]
    for result in queryResults:
        results.append([result[i].strftime("%m_%d_%Y") if isinstance(result[i], datetime.datetime) or isinstance(result[i], datetime.date) else ("NULL" if result[i] is None else ("EMPTY" if str(result[i]).strip() == "" else result[i])) for i in range(numFields)])

    return results

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
def chirp_replica(entry, replicas, fileInfo, filename, filePath):
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

            if md5sum == fileInfo['checksum']: # file successfully chirped
                return 0
            else:
                os.remove(filePath)     # remove file with incorrect md5sum
                print(f"Warning: {filename} from host {host} does not match md5sum in metadata\n         removing invalid file from filesystem")

        print(f"Warning: {filename} failed to be retreived from host {host}")
        failedHosts.add(host)

    print(f"Error: {filename} could not be retreived")
    return 1

# return sorted dictionary of replica metadata
def query_replicas(fileids, connection):
    replicas = query("select * from replicas where fileid in ({})".format(','.join(map(str, fileids))), connection)

    keys = replicas[0]
    replicas = [dict(zip(keys, entry)) for entry in replicas[1:] ]

    sortedReplicas = {}
    for replica in replicas:
        if replica["fileid"] not in sortedReplicas:
            sortedReplicas[replica["fileid"]] = [replica]
        else:
            sortedReplicas[replica["fileid"]].append(replica)

    return sortedReplicas

def query_files(fileids, connection):
    files = query("select * from files where fileid in ({})".format(','.join(map(str, fileids))), connection)

    keys = files[0]
    files = [dict(zip(keys, entry)) for entry in files[1:] ]

    sortedFiles = {}
    for file in files:
        sortedFiles[file["fileid"]] = file

    return sortedFiles

# chirp files into filesystem
def chirp_files(queryData, headdir, schema):
    fileids = [entry["fileid"] for entry in queryData]

    connection = connect()
    if not connection: # failed to connect
        print("failed to connect to ccldb.crc.nd.edu biometrics database")
        return 1

    replicas = query_replicas(fileids, connection)
    files = query_files(fileids, connection)

    for entry in tqdm(queryData, total = len(queryData), desc="chirping files with metadata"):
        dirPath = headdir + '/'.join([str(entry[attribute]) for attribute in schema]) + '/'
        if not os.path.exists(dirPath): # create directory branch
            os.makedirs(dirPath)

        fileid = entry["fileid"]
        filename = str(fileid) + "." + files[fileid]["extension"]
        filePath = dirPath + filename

        if chirp_replica(entry, replicas[fileid], files[fileid], filename, filePath) == 0:
            write_metadata(entry, filename, dirPath, filePath, headdir + "metadata.json")

def construct_tree_nochirp(queryData, headdir, schema):
    fileids = [entry["fileid"] for entry in queryData]

    connection = connect()
    if not connection: # failed to connect
        print("failed to connect to ccldb.crc.nd.edu biometrics database")
        return 1

    files = query_files(fileids, connection)

    for entry in tqdm(queryData, total = len(queryData), desc="constructing filesystem (metadata only)"):
        dirPath = headdir + '/'.join([str(entry[attribute]) for attribute in schema]) + '/'
        if not os.path.exists(dirPath): # create directory branch
            os.makedirs(dirPath)

        fileid = entry["fileid"]
        filename = str(fileid) + "." + files[fileid]["extension"]
        filePath = dirPath + filename

        write_metadata(entry, filename, dirPath, filePath, headdir + "metadata.json", chirping=False)

def write_metadata(dataEntry, filename, dirPath, filePath, largeMetadata, chirping = True):
    # store metadata in leaf json
    write_file_json(dirPath + '/metadata_refined.json', dataEntry, filename)

    # store metadata in root json
    with open(largeMetadata, 'r') as metadata:
        data = json.load(metadata)
        if chirping:
            data['chirped-fileids'].append(dataEntry["fileid"])
        data['files'][filename] = {'path': filePath,
                                   'metadata': dataEntry}
    with open(largeMetadata, 'w') as metadata:
        metadata.write(json.dumps(data, indent=4))
    return 0

