#!/usr/bin/env python3

import database

import sys
import re
import os
import subprocess
import json
from datetime import datetime

SUBJECTS = ["subjectid", "gender", "source1", "YOB", "source2", "race", "source3", "sequence", "date_added", "s_irises", "ethnicity"]

TABLES = {
         "faces_3d": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "subjectid", "glasses", "source1", "emotion", "source2", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "illuminantid2", "fileid", "state", "by_user", "date_added", "added_by", "temp_collectionid", "enrolled_date", "yaw"],

          "faces_mov": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "emotion", "source2", "rank", "talking", "action", "head", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "fileid", "state", "by_user", "date_added", "added_by", "temp_collectionid", "enrolled_date"],

          "faces_still": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "emotion", "source2", "rank", "lefteye", "righteye", "nose", "mouth", "yaw", "pitch", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "illuminantid2", "state", "fileid", "by_user", "date_added", "added_by", "comment", "temp_collectionid", "enrolled_date"],

          "irises_mov": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "eye", "color", "pose", "motion", "treatment", "conditions", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "fileid", "state", "by_user", "date_added", "added_by", "temp_collectionid", "enrolled_date", "contacts", "contacts_type", "contacts_texture", "contacts_toric", "contacts_cosmetic"],

          "irises_still": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "eye", "color", "pose", "motion", "treatment", "coordinate00", "coordinate01", "coordinate02", "coordinate03", "coordinate04", "coordinate05", "coordinate06", "coordinate07", "coordinate08", "coordinate09", "coordinate10", "coordinate11", "coordinate12", "coordinate13", "coordinate14", "coordinate15", "coordinate16", "glasses", "collectionid", "stageid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "weather", "shot", "state", "fileid", "by_user", "date_added", "added_by", "comment", "temp_collectionid", "enrolled_date", "contacts", "contacts_type", "contacts_texture", "contacts_toric", "contacts_cosmetic"]
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
                         SCHEME in form attr1/attr2/... where attr's in TABLE attributes
                         (ex: -schema subjectid/date)
    -group GROUPS N      select at most N files for each group according to GROUPS scheme
                         GROUPS in form attr1/arrt2/... where attr's in TABLE attributes
                         (ex: -group subjectid/eye 2)
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
                         useful when materialization ends prematurely
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
        force[0] = 0 # treat materialization as normal since headdir does not already exist
    return True

def materialize(schema, groups, headdir, limit, conditions, force, nofiles, tablename):
    connection = database.connect('ccldb.crc.nd.edu', 'biometrics')
    if not connection:
        print("Error: Invalid credentials provided")
        return 1
    root = os.getcwd() + f'/{headdir}/'

    whereClause = "WHERE " + conditions if conditions else ""
    if conditions: whereClaues.replace("subjectid", f"{tablename}.subjectid").replace("fileid", f"{tablename}.fileid") # avoid ambiguity in where clauses
    limitClause = "limit " + str(limit) if limit != None else ""
    files = database.query(f"select * from {tablename} inner join files on {tablename}.fileid = files.fileid inner join subjects on {tablename}.subjectid = subjects.subjectid {whereClause} {limitClause};", connection)

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

def save_materialization(commandLine, schema, groups, headdir, limit, conditions, force, nofiles, tablename):
    query = {"commandLine": commandLine,
             "schema": schema,
             "groups": groups,
             "headdir": headdir,
             "limit": limit,
             "conditions": conditions,
             "force": force,
             "nofiles": nofiles,
             "tablename": tablename}
    bxgridDirectory = os.path.expanduser('~') + "/.bxgrid/"
    historyFile = bxgridDirectory + "history_materializations.json"
    lastMaterializationFile = bxgridDirectory + "latest_materialization.json"

    # establish ~/.bxgrid/ directory
    if not os.path.exists(bxgridDirectory):
        os.makedirs(bxgridDirectory)
    # create history_materializations.json if not in ~/.bxgrid/
    if not os.path.exists(historyFile):
        with open(historyFile, "w") as history:
            history.write(json.dumps({"history": []}, indent=4))

    # add query to history
    with open(historyFile, "r") as history:
        data = json.load(history)
        data["history"].append(query)
        if len(data["history"]) > 500: # keep last 500 queries
            data["history"].pop(0)
    with open(historyFile, "w") as updatedHistory:
        updatedHistory.write(json.dumps(data, indent=4))

    # save query as last materialization
    with open(lastMaterializationFile, "w") as latest:
        latest.write(json.dumps(query, indent=4))

def last_materialization():
    historyFile = os.path.expanduser('~') + "/.bxgrid/latest_materialization.json"
    if os.path.exists(historyFile): # materialize last query
        with open(historyFile, 'r') as history:
            query = json.load(history)
            force = [True]
            if materializer_precheck(query["tablename"], query["schema"], query["headdir"], force):
                return materialize(query["schema"], query["groups"], query["headdir"], query["limit"], query["conditions"], force[0], query["nofiles"], query["tablename"])
    else: # history file unavailable
        print("Error: no past materialization to resume")
        return 1

def main():
    # parse materializer command line input
    commandLine = " ".join(sys.argv)
    arguments = sys.argv[1:]
    schema = []
    groups = []
    groupsN = None
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
        elif argument == '-groups':
            groups = arguments.pop(0).split('/')
            groupsN = int(arguments.pop(0))
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
    if tablename not in TABLES or (not all(attr in TABLES[tablename] for attr in schema)) or (not all(attr in (TABLES[tablename] + SUBJECTS) for attr in groups)):
        usage(1)
        return 1
    if headdir == "":
        headdir = tablename + "-" + datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")

    # save materialization arguments to ~/.bxgrid/history.json
    save_materialization(commandLine, schema, groups, headdir, limit, conditions, force, nofiles, tablename)

    # materialize
    if materializer_precheck(tablename, schema, headdir, force):
        return materialize(schema, groups, headdir, limit, conditions, force[0], nofiles, tablename)

    return 1

if __name__ == "__main__":
    main()
