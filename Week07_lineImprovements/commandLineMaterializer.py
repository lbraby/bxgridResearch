#!/usr/bin/env python3

import database

import sys
import re
import os
import subprocess
import json
from datetime import datetime

TABLES = {
         "faces_3d": ["id", "recordingid", "shotid", "sequenceid", "url", "date", "format", "subjectid", "glasses", "source1", "emotion", "source2", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "illuminantid2", "fileid", "state", "by_user", "lastcheck", "date_added", "added_by", "temp_collectionid", "enrolled_date", "yaw"],

          "faces_mov": ["id", "recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "emotion", "source2", "rank", "talking", "action", "head", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "fileid", "state", "by_user", "lastcheck", "date_added", "added_by", "temp_collectionid", "enrolled_date"],

          "faces_still": ["id", "recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "emotion", "source2", "rank", "lefteye", "righteye", "nose", "mouth", "yaw", "pitch", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "illuminantid2", "state", "fileid", "by_user", "lastcheck", "date_added", "added_by", "comment", "temp_collectionid", "enrolled_date"],

          "irises_mov": ["id", "recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "eye", "color", "pose", "motion", "treatment", "conditions", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "fileid", "state", "by_user", "lastcheck", "date_added", "added_by", "temp_collectionid", "enrolled_date", "contacts", "contacts_type", "contacts_texture", "contacts_toric", "contacts_cosmetic"],

          "irises_still": ["id", "recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "eye", "color", "pose", "motion", "treatment", "condition", "coordinate00", "coordinate01", "coordinate02", "coordinate03", "coordinate04", "coordinate05", "coordinate06", "coordinate07", "coordinate08", "coordinate09", "coordinate10", "coordinate11", "coordinate12", "coordinate13", "coordinate14", "coordinate15", "coordinate16", "glasses", "collectionid", "stageid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "weather", "shot", "state", "fileid", "by_user", "lastcheck", "date_added", "added_by", "comment", "temp_collectionid", "enrolled_date", "contacts", "contacts_type", "contacts_texture", "contacts_toric", "contacts_cosmetic"]
           }

FILE_ATTRIBUTES = ["fileid", "userkey", "checksum", "extension", "size", "lastcheck", "rank", "fstate"]

def usage(status=0):
    tableAttributes = ""
    for table in TABLES:
        tableAttributes = tableAttributes + "\t" + table + " : \n\t\t" + ", ".join(TABLES[table]) + "\n"
    command = os.path.basename(sys.argv[0])
    print(f'''Usage: {command} [options] TABLE
Tables and attributes:
{tableAttributes}
Examples:
    {command} -schema subjectid/emotion -where "date like '%2002-09-17%'" -dryrun faces_still
        faces_still-2022_10_12-02_43_32_PM
        ├── metadata.json
        ├── nd1S02463
        │   ├── BlankStare
        │   │   └── metadata_refined.json
        │   └── Happiness
        │       └── metadata_refined.json
        ├── nd1S04201
        │   ├── BlankStare
        │   │   └── metadata_refined.json
        │   └── Happiness
        │       └── metadata_refined.json
        ┆
        ├── nd1S04573
        │   ├── BlankStare
        │   │   └── metadata_refined.json
        │   └── Happiness
        │       └── metadata_refined.json
        └── nd1S04579
            ├── BlankStare
            │   └── metadata_refined.json
            └── Happiness
                └── metadata_refined.json
    {command} -headdir irises_mov -schema color/eye -limit 30 irises_mov
        irises_mov
        ├── Blue
        │   ├── Left
        │   │   ├── 245861.mp4
        │   │   ├── 245863.mp4
        │   │   └── metadata_refined.json
        │   └── Right
        │       ├── 245862.mp4
        │       └── metadata_refined.json
        ├── Brown
        │   ├── Left
        │   │   ├── 245853.mp4
        │   │   └── metadata_refined.json
        │   └── Right
        │       ├── 245854.mp4
        │       └── metadata_refined.json
        ├── Green
        │   ├── Left
        │   │   ├── 245857.mp4
        │   │   └── metadata_refined.json
        │   └── Right
        │       ├── 245858.mp4
        │       ├── 245860.mp4
        │       └── metadata_refined.json
        ├── Hazel
        │   ├── Left
        │   │   ├── 245855.mp4
        │   │   └── metadata_refined.json
        │   └── Right
        │       ├── 245856.mp4
        │       └── metadata_refined.json
        └── metadata.json
Options:
    -credentials         change stored credentials for accessing bxgrid
                         credentials saved to ~/.bxgrid/credentials after successful login
    -headdir DIR         set name of top level directory for materialization
                         default DIR: TABLE + timestamp
    -schema  SCHEME      set schema for directory tree;
                         SCHEME in form attr1/attr2/... where attr's in TABLE's attributes
                         (ex: -schema subjectid/date)
    -limit   N           set maximum number of files to download
    -where   CONDITIONS  limit materialization to files fulfilling conditions;
                         condtions written in SQL WHERE clause syntax
                         (ex: -where "subjectid='nd1S04261' and date='2008-04-01'")
    -force               allow materialization into existing directory
                         as long as schemas match
    -nofiles             run materialization without downloading files
                         (only tree and metadata files created)
    -resume              execute last materialization from the beginning
                         metadata and files will not be overwritten
                         materialization will be run as if -force flag present
          ''')
    sys.exit(status)

def materializer_precheck(tablename, schema, headdir, force):
    cwd = os.getcwd() + '/'
    if os.path.isdir(cwd + headdir):
        if not force[0]:
            print(f'''Materialization Error: The directory {headdir} already exists
To materialize into an already existing directory use -force flag and a schema equivalent to preexisting schema''')
            return False

        metadataFile = cwd + headdir + '/metadata.json'
        if not os.path.isfile(metadataFile):
            print(f'''Materialization Error: The directory {headdir} does not contain metadata.json''')
            return False
        with open(metadataFile, 'r') as metadata:
            data = json.load(metadata)
            if data["schema"] != schema:
                print(f'''Materialization Error: schema does not match prexisting schema of directory {headdir}''')
                return False
            if data["table"] != tablename:
                print(f'''Materialization Error: table does not match table of directory {headdir}''')
    else:
        force[0] = 0    # treat materialization as normal since headdir does not already exist
    return True

def materialize(schema, headdir, limit, conditions, force, nofiles, tablename):
    connection = database.connect('ccldb.crc.nd.edu', 'biometrics')
    if not connection:
        print("Error: Invalid credentials provided")
        return 1
    root = os.getcwd() + f'/{headdir}/'

    whereClause = "WHERE " + conditions if conditions else ""
    limitClause = "limit " + str(limit) if limit != None else ""
    files = database.query(f"select * from {tablename} inner join files on {tablename}.fileid = files.fileid {whereClause} {limitClause};", connection)

    # create dictionary of all queried fileids
    fileidsToChirp = {}
    for file in files:
        fileidsToChirp[file["fileid"]] = file

    if not force: # create headdir and it's metadata file if none exists
        os.makedirs(root)
        with open(f'{root}metadata.json', 'w') as metadata:
            metadata.write(json.dumps({"table": tablename,
                                       "schema": schema,
                                       "files": {},
                                       "subjects": {},
                                       "chirped-fileids": []}, indent=4))

    else:         # check which files have already been chirped
        with open(f'{root}metadata.json', 'r') as metadata:
            data = json.load(metadata)
            for fileid in data["chirped-fileids"]:
                if fileid in fileidsToChirp.keys():
                    del fileidsToChirp[fileid]

    if fileidsToChirp:
        subprocess.call(['chmod', '-R', '777', root])   # allow for changes to directory tree
        database.chirp_files(fileidsToChirp, root, schema, connection, nofiles)
        database.write_subjects_info(root, connection)
        subprocess.call(['chmod', '-R', '444', root])   # restrict permissions to read-only

def save_materialization(schema, headdir, limit, conditions, force, nofiles, tablename):
    query = {"schema": schema,
             "headdir": headdir,
             "limit": limit,
             "conditions": conditions,
             "force": force,
             "nofiles": nofiles,
             "tablename": tablename}
    bxgridDirectory = os.path.expanduser('~') + "/.bxgrid/"
    historyFile = bxgridDirectory + "history.json"

    if not os.path.exists(bxgridDirectory): # create ~/.bxgrid/
        os.makedirs(bxgridDirectory)
    if not os.path.exists(historyFile):
        with open(historyFile, "w") as history:
            history.write(json.dumps({"history": []}, indent=4))

    with open(historyFile, "r") as history:
        data = json.load(history)
        data["history"].append(query)
    with open(historyFile, "w") as updatedHistory:
        updatedHistory.write(json.dumps(data, indent=4))

def last_materialization():
    historyFile = os.path.expanduser('~') + "/.bxgrid/history.json"
    if os.path.exists(historyFile): # materialize last query
        with open(historyFile, 'r') as history:
            data = json.load(history)
            query = data["history"][-1]
            force = [True]
            if materializer_precheck(query["tablename"], query["schema"], query["headdir"], force):
                return materialize(query["schema"], query["headdir"], query["limit"], query["conditions"], force[0], query["nofiles"], query["tablename"])
    else: # history file unavailable
        print("Error: no past materialization to resume")
        return 1

def main():
    # parse materializer command line input
    arguments = sys.argv[1:]
    schema = []
    headdir = ""
    limit = None
    conditions = ""
    force = [0] # list allows force be modified within materializer_precheck()
    nofiles = False
    tablename = ""

    if len(arguments) == 0:
        usage(1)
        return 1
    while arguments and arguments[0].startswith('-'):
        argument = arguments.pop(0)
        if argument == '-help':
            usage(0)
            return 1
        elif argument == '-resume':
            return last_materialization()
        elif argument == '-credentials': # require user to re-eneter login info
            if os.path.exists(os.path.expanduser('~') + "/.bxgrid/credentials"):
                os.remove(os.path.expanduser('~') + "/.bxgrid/credentials")
        elif argument == '-headdir':
            headdir = arguments.pop(0)
        elif argument == '-schema':
            schema = arguments.pop(0).split('/')
        elif argument == '-limit':
            limit = int(arguments.pop(0))
        elif argument == '-where':
            conditions = arguments.pop(0)
        elif argument == '-force':
            force = [1]
        elif argument == '-nofiles':
            nofiles = True
        else:
            usage(1)
            return 1
    if len(arguments) == 0:
        usage(1)
        return 1
    tablename = arguments.pop(0)
    if tablename not in TABLES or not all(branch in TABLES[tablename] for branch in schema):
        usage(1)
        return 1
    if headdir == "":
        headdir = tablename + "-" + datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")

    # save materialization arguments to ~/.bxgrid/history.json
    save_materialization(schema, headdir, limit, conditions, force, nofiles, tablename)

    # materialize
    if materializer_precheck(tablename, schema, headdir, force):
        return materialize(schema, headdir, limit, conditions, force[0], nofiles, tablename)

    return 1

if __name__ == "__main__":
    main()
