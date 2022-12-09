#!/usr/bin/env python3

import database

import os
import sys
import csv
from typing import List
from datetime import datetime

SUBJECTS_ATTRIBUTES = ["subjectid", "gender", "source1", "YOB", "source2", "race", "source3", "sequence", "date_added", "s_irises", "ethnicity"]

TABLES = {
         "faces_3d": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "subjectid", "glasses", "source1", "emotion", "source2", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "illuminantid2", "fileid", "state", "by_user", "date_added", "added_by", "temp_collectionid", "enrolled_date", "yaw"],

          "faces_mov": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "emotion", "source2", "rank", "talking", "action", "head", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "fileid", "state", "by_user", "date_added", "added_by", "temp_collectionid", "enrolled_date"],

          "faces_still": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "emotion", "source2", "rank", "lefteye", "righteye", "nose", "mouth", "yaw", "pitch", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "illuminantid2", "state", "fileid", "by_user", "date_added", "added_by", "comment", "temp_collectionid", "enrolled_date"],

          "irises_mov": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "glasses", "source1", "eye", "color", "pose", "motion", "treatment", "conditions", "stageid", "weather", "collectionid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "fileid", "state", "by_user", "date_added", "added_by", "temp_collectionid", "enrolled_date", "contacts", "contacts_type", "contacts_texture", "contacts_toric", "contacts_cosmetic"],

          "irises_still": ["recordingid", "shotid", "sequenceid", "url", "date", "format", "camera", "subjectid", "eye", "color", "pose", "motion", "treatment", "coordinate00", "coordinate01", "coordinate02", "coordinate03", "coordinate04", "coordinate05", "coordinate06", "coordinate07", "coordinate08", "coordinate09", "coordinate10", "coordinate11", "coordinate12", "coordinate13", "coordinate14", "coordinate15", "coordinate16", "glasses", "collectionid", "stageid", "environmentid", "sensorid", "illuminantid", "illuminantid1", "weather", "shot", "state", "fileid", "by_user", "date_added", "added_by", "comment", "temp_collectionid", "enrolled_date", "contacts", "contacts_type", "contacts_texture", "contacts_toric", "contacts_cosmetic"]
           }

FILE_ATTRIBUTES = ["fileid", "userkey", "checksum", "extension", "size", "lastcheck", "rank", "fstate"]

def usage(status=0):
    command = os.path.basename(sys.argv[0])
    print(f"""Usage: {command} [option...] TABLE
Pass '-help' flag for more information""")
    sys.exit(status)

def help(status=0):
    tableAttributes = ""
    spacing = "    "
    for table in TABLES:
        tableAttributes = tableAttributes + spacing + table + f":\n{spacing}{spacing}" + ", ".join(TABLES[table]) + "\n\n"
    fileAttributes = spacing + f"files:\n{spacing}{spacing}" + ", ".join(FILE_ATTRIBUTES)
    subjectsAttributes = spacing + f"subjects:\n{spacing}{spacing}" + ", ".join(SUBJECTS_ATTRIBUTES)
    command = os.path.basename(sys.argv[0])
    print(f'''Usage: {command} [option...] TABLE
Query data from bxgrid's biometrics database and store in csv file
Example: {command} -limit 100 -outcsv test.csv faces_still

Options:
    Required:
        -limit   N           must specify maximum number of entries to query
                             '-limit unlimited' to query all entries
    Optional:
        -outcsv  FILENAME    specify output file
                             if outcsv not specified, FILENAME = query_{{timestamp}}.csv
        -mysql   QUERY       use custom mysql query
                             does not require TABLE to be specified
                             (ex: -mysql "select * from faces_still inner join subjects on subjectid")
        -where   CONDITIONS  limit query to files fulfilling conditions;
                             condtions written in SQL WHERE clause syntax
                             (ex: -where "subjectid='nd1S04261' and date='2008-04-01'")
        -schema  ATTRIBUTES  select ATTRIBUTES (in form attr1/attr2/...) to query
                             by default, all attributes selected
                             attributes may come from TABLE, file or subjects (consult documentation)
        -overwrite           if output file already exists, overwrite
        -credentials         do not use previously saved credentials to access bxgrid

Tables: faces_still, faces_3d, faces_mov, irises_still, irises_mov
          ''')
    sys.exit(status)

def export_query(customQuery: str, outfile: str):
    connection = database.connect()
    if not connection: # failed to connect
        print("failed to connect to ccldb.crc.nd.edu biometrics database")
        return 1

    results = database.query(customQuery, connection)
    if results == None: # query failed
        return 1

    with open(outfile, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(results[0])
        csvwriter.writerows(results[1:])

    print(f"query: saved data successfully in {os.path.relpath(outfile)}")
    return 0

def export(tablename: str, limit: int, conditions: str, attributes: List[str], outfile: str):
    whereClause = "WHERE " + conditions if conditions else ""
    if conditions: whereClause = whereClause.replace("subjectid", f"{tablename}.subjectid").replace("fileid", f"{tablename}.fileid") # avoid ambiguity in where clauses
    limitClause = "limit " + str(limit) if limit != None else ""
    # validate attributes
    for attribute in attributes:
        if (attribute not in TABLES[tablename] and attribute not in SUBJECTS_ATTRIBUTES + FILE_ATTRIBUTES) and attribute != '*':
            print(f"query: invalid attribute '{attribute}' specified")
            usage(1)
    attributes = ', '.join(attributes).replace("subjectid", f"{tablename}.subjectid").replace("fileid", f"{tablename}.fileid")

    query = f"select {attributes} from {tablename} inner join files on {tablename}.fileid = files.fileid inner join subjects on {tablename}.subjectid = subjects.subjectid {whereClause} {limitClause};"

    return export_query(query, outfile)

def main():
    commandLine = " ".join(sys.argv)
    arguments = sys.argv[1:]
    outfile = ""
    limit = None
    conditions = ""
    attributes = ["*"]
    customQuery = ""
    overwrite = False
    tablename = ""

    if len(arguments) == 0:
        usage(1)
    # parse command line arguments
    while arguments and arguments[0].startswith('-'):
        argument = arguments.pop(0)
        if argument == '-help':
            help(0)
        if argument == '-outcsv':
            outfile = arguments.pop(0)
        elif argument == '-limit':
            lim = (arguments.pop(0))
            if lim == "unlimited": limit = sys.maxsize
            else:
                try:
                    limit = int(lim)
                except ValueError:
                    print("query: limit argument may only 'unlimited' or a valid numeric")
                    usage(1)
        elif argument == '-where':
            conditions = arguments.pop(0)
        elif argument == '-schema':
            attributes = arguments.pop(0).split('/')
        elif argument == '-mysql':
            customQuery = arguments.pop(0)
        elif argument == '-credentials': # require user to re-eneter login info
            if os.path.exists(os.path.expanduser('~') + "/.bxgrid/credentials"):
                os.remove(os.path.expanduser('~') + "/.bxgrid/credentials")
        elif argument == '-overwrite':
            overwrite = True
        else:
            print(f"query: unrecognized option '{argument}'")
            usage(1)

    if len(arguments) == 0 and not customQuery:
        print("query: missing TABLE argument\nTry '-help' flag for more information")
        return 1

    if not customQuery:
        tablename = arguments.pop(0)
        if tablename not in TABLES:
            print(f"query: invalid TABLE '{tablename}' specified")
            usage(1)

    if limit == None and not customQuery:
        print("query: must specify limit using '-limit' flag")
        usage(1)

    if not outfile: # default output file name
        outfile = "query_" + datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p") + ".csv"
    outfile = os.path.abspath(outfile)

    if not overwrite and os.path.exists(outfile):
        print(f"query: outcsv {os.path.relpath(outfile, start = os.curdir)} already exists\n       use '-overwrite' to overwrite file")
        usage(1)

    if customQuery:
        return export_query(customQuery, outfile)
    else:
        return export(tablename, limit, conditions, attributes, outfile)


if __name__ == "__main__":
    main()
