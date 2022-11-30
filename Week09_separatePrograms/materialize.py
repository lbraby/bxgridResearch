#!/usr/bin/env python3

import database

import sys
import os
import csv
import json
from datetime import datetime

def usage(status = 0):
    command = os.path.basename(sys.argv[0])
    print(f'''Usage: {command} [options] CSVFILE
Examples:

Options:
    -headdir DIR         set name of top level directory for materialization
                         default DIR: materialization_{{timestamp}}
    -schema  SCHEME      set schema for directory tree
                         SCHEME in form attr1/attr2/... where attr is column in CSVFILE
                         (ex: -schema subjectid/date)
    -fileid  ATTR        specify which field contains fileids for chirping
                         default ATTR: fileid
                         use when fileid attribute has alias
    -force               allow materialization into existing directory as long as schemas match
    -nofiles             run materialization without downloading files
    -resume              execute last materialization from the beginning
                         materialization will be run as if -force flag present
                         useful when materialization ends prematurely
                         files will not be overwritten
          ''')
    sys.exit(status)

def floatAttempt(num: str):
    if num.isdigit():
        return int(num)
    elif num.replace('.', '', 1).isdigit():
        return float(num)
    else:
        return num

def save_materialization(commandLine, csvfile, headdir, schema, fileid, force, nofiles):
    query = {"command line": commandLine,
             "csv input file": csvfile,
             "headdir": headdir,
             "schema": schema,
             "fileid alias": fileid,
             "force": force,
             "nofiles": nofiles}
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
    # add query to history_materializations.json
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

def materialization_precheck(csvfile, headdir, schema, fileid, force, nofiles):
    if not os.path.isfile(csvfile):
        print(f"materialize: cannot access '{os.path.relpath(csvfile, start = os.curdir)}': No such file")
        return False

    if os.path.isdir(headdir):
        if not force[0]:
            print(f"materialize: directory '{os.path.relpath(headdir, start = os.curdir)}' already exists\n             to materialize into already existing directory, use '-force' flag and equivalent schema")
            return False

        largeMetadata = headdir + 'metadata.json'
        if not os.path.isfile(largeMetadata):
            print(f"materialize: directory '{os.path.relpath(headdir, start = os.curdir)}' does not contain metadata.json")
            return False
        with open(largeMetadata, 'r') as metadata:
            data = json.load(metadata)
            if data["schema"] != schema:
                print(f"materialize: schema does not match that of already existing directory '{os.path.relpath(headdir, start = os.curdir)}'\n             to specify a new schema, use '-schema' flag")
                return False
    else:
        force[0] = False

    with open(csvfile, newline='') as file:
        attributes = next(csv.reader(file))
        if not all(attr in attributes for attr in schema):
            print(f"materialize: at lease one attribute in schema not found in '{os.path.relpath(csvfile, start = os.curdir)}'")
            return False

    return True

def materialize(csvfile, headdir, schema, fileid, force, nofiles):
    force = [force]
    if not materialization_precheck(csvfile, headdir, schema, fileid, force, nofiles):
        return 1
    force = force[0]

    connection = database.connect()
    if not connection: # failed to connect
        print("failed to connect to ccldb.crc.nd.edu biometrics database")
        return 1

    with open(csvfile) as file:
        csvData = [{"fileid" if k == fileid else k: floatAttempt(v) for k, v in row.items()} for row in csv.DictReader(file, skipinitialspace=True)]

    if not force:
        os.makedirs(headdir)
        with open(f'{headdir}metadata.json', 'w') as metadata:
            metadata.write(json.dumps({"csv file": csvfile,
                                       "schema": schema,
                                       "files": {},
                                       "chirped-fileids": []}, indent=4))
    else: # limit chirping to files not already in directory tree
        with open(f'{headdir}metadata.json', 'r') as metadata:
            data = json.load(metadata)
            for fileid in data["chirped-fileids"]:
                for i in range(len(csvData)):
                    if fileid == csvData[i]["fileid"]:
                        del csvData[i]
                        break
    if not csvData:
        return 0

    if not nofiles:
        database.chirp_files(csvData, headdir, schema)
    else:
        database.construct_tree_nochirp(csvData, headdir, schema)

    return 0

def main():
    commandLine = " ".join(sys.argv)
    arguments = sys.argv[1:]
    headdir = ""
    schema = []
    fileid = "fileid"
    force = False
    nofiles = False
    csvfile = ""

    if len(arguments) == 0:
        usage(1)
        return 1
    # parse command line arguments
    while arguments and arguments[0].startswith('-'):
        argument = arguments.pop(0)
        if argument == '-help':
            usage(0)
            return 1
        elif argument == '-headdir':
            headdir = arguments.pop(0)
        elif argument == '-schema':
            schema = arguments.pop(0).split('/')
        elif argument == '-fileid':
            fileid = arguments.pop(0)
        elif argument == '-force':
            force = True
        elif argument == '-nofiles':
            nofiles = True
        elif argument == '-resume':
            try:
                with open(os.path.expanduser('~') + "/.bxgrid/latest_materialization.json", 'r') as latest:
                    data = json.load(latest)
                    return materialize(data["csv input file"], data["headdir"], data["schema"], data["fileid alias"], True, data["nofiles"])
            except IOError:
                print("materialize: no materialization to resume")
                return 1

        else:
            usage(1)
            return 1
    if len(arguments) == 0:
        print("materialize: missing CSVFILE argument\nTry '-help' flag for more information")
        return 1
    csvfile = os.path.abspath(arguments.pop(0))
    if not os.path.exists(csvfile):
        print("materialize: specified csv file does not exist\nTry a different file or make sure file exists")
        return 1

    if not headdir:
        headdir = "materialization" + datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
    headdirPath = os.path.abspath(headdir) + '/'

    save_materialization(commandLine, csvfile, headdirPath, schema, fileid, force, nofiles)

    return materialize(csvfile, headdirPath, schema, fileid, force, nofiles)

if __name__ == "__main__":
    main()

