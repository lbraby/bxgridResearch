#!/usr/bin/env python3

import re
import os
import json
import time
import Chirp
import getpass
import datetime
import subprocess
import mysql.connector
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
        print("Error executing MySQL query:", str(err).split(":", 1)[1].strip())
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
connectedHosts = {}
def chirp_replica(entry, replicas, fileInfo, existingFiles, filename, filePath, smartchirp):
    fileid = str(fileInfo["fileid"])

    if smartchirp and (str(fileid) in existingFiles):
        existingFile = existingFiles[fileid]
        if os.path.isfile(existingFile):
            if os.popen(f'md5sum {existingFile}').read().split()[0] == fileInfo["checksum"]:
                os.popen(f'cp {existingFile} {filePath}')
                return 0
            else:
                print(f"Warning: md5sum of {filename} saved locally does not match that in bxgrid")

    skippedHosts = set()
    # attempt chirp until file successfully chirped
    for replica in replicas:
        host = replica['host']
        path = replica['path']

        if host in failedHosts and host not in skippedHosts:
            skippedHosts.add(host)
            replicas.append(replica) # will attempt to chirp from host if all other hosts fail
            continue

        if host in connectedHosts:
            client = connectedHosts[host]
        else:
            try:
                client = Chirp.Client(host, authentication = ['unix'])
                connectedHosts[host] = client
            except Chirp.AuthenticationFailure:
                print(f"Warning: failed to connect to host {host}")
                failedHosts.add(host)
                continue
        try:
            client.get(path, filePath)
            md5sum = os.popen(f'md5sum {filePath}').read().split()[0]

            if md5sum == fileInfo['checksum']: # file successfully chirped
                existingFiles[fileid] = filePath
                return 0
            else:
                os.remove(filePath)     # remove file with incorrect md5sum
                print(f"Warning: {filename} from host {host} does not match md5sum in metadata\n         removing invalid file from filesystem")
        except:
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
def chirp_files(queryData, headdir, schema, smartchirp):
    fileids = [entry["fileid"] for entry in queryData]
    rates = []
    rate = "0 MB/s"

    connection = connect()
    if not connection: # failed to connect
        print("failed to connect to ccldb.crc.nd.edu biometrics database")
        return 1

    replicas = query_replicas(fileids, connection)
    files = query_files(fileids, connection)

    with open(os.path.expanduser('~') + "/.bxgrid/chirpedFiles.json", "r") as file:
        existingFiles = json.load(file)

    for i, entry in enumerate(tqdm(queryData, total = len(queryData), desc="chirping files with metadata")):
        if i % 50 == 0:
            with open(os.path.expanduser('~') + "/.bxgrid/chirpedFiles.json", "w") as file:
                file.write(json.dumps(existingFiles, indent=4))

        dirPath = headdir + '/'.join([str(entry[attribute]) for attribute in schema]) + ('/' if schema else '')
        if not os.path.exists(dirPath): # create directory branch
            os.makedirs(dirPath)

        fileid = entry["fileid"]
        filename = str(fileid) + "." + files[fileid]["extension"]
        filePath = dirPath + filename

        if chirp_replica(entry, replicas[fileid], files[fileid], existingFiles, filename, filePath, smartchirp) == 0:
            write_metadata(entry, filename, dirPath, filePath, headdir + "metadata.json")

    with open(os.path.expanduser('~') + "/.bxgrid/chirpedFiles.json", "w") as file:
        file.write(json.dumps(existingFiles, indent=4))

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
