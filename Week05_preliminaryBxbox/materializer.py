#!/usr/bin/env python3

import database

import sys
import re
import os
import json

TABLES = ["faces_3d", "faces_mov", "faces_still", "irises_mov", "irises_still"]
REPLICA_ATTRIBUTES = ["replicaid", "fileid", "host", "path", "state", "lastcheck"]
FILE_ATTRIBUTES = ["fileid", "userkey", "checksum", "extension", "size", "lastcheck", "rank", "fstate"]

# prompt user for query and execute
# syntax: materialize {table} as {filesystem schema} {SQL where, limit statements}
def bxbox_query(connection):
    print("bxbox> ", end="")
    query = ""
    while ';' not in query: # read in query from user
        query += " " + input()

    # validate syntax
    command = re.search('materialize[ ]*([^ ]*)[ ]*as[ ]*([^ ]*(/[^ ])*)[ ]*(.*);', query.strip(), re.IGNORECASE)
    if not command:
        print("ERROR: invalid bxbox syntax used")
        return 1 # invalid bxbox syntax

    # validate table
    table = command.group(1)
    if table not in TABLES:
        print("ERROR: invalid table selected")
        return 1 # invalid table queried

    # validate filesystem schema
    validAttributes = database.describe(table, connection)
    schema = command.group(2).split('/')
    if(not all(attribute in validAttributes for attribute in schema)):
        print("ERROR: invalid filesystem schema")
        return 1 # at least one invalid attribute in schema

    scope = command.group(4)

    if not bxbox_execute(connection, table, schema, scope, validAttributes):
        return 1 # invalid sql execution

    return 0

def bxbox_execute(connection, table, schema, scope, tableAttributes):
    sqlQuery = f'select * from {table} left join files using (fileid) left join replicas using (fileid) {scope};'

    results = database.query(sqlQuery, connection)
    if not results:
        return 1; # query failed

    replicaResults = database.filter_replicas(results, table, REPLICA_ATTRIBUTES)
    fileResults = database.filter_files(results, table, tableAttributes + FILE_ATTRIBUTES)

    #with open("replicas.json", 'w') as newfile:
    #    newfile.write(json.dumps(replicaResults, indent=4))
    #with open("files.json", 'w') as newfile:
    #    newfile.write(json.dumps(fileResults, indent=4))
    inputtedSchema = '/'.join(schema)
    header = {'bxbox query' : f'materialize {table} as {inputtedSchema} {scope}', 'MySQL query' : sqlQuery}

    database.chirp_files(fileResults, replicaResults, table, schema)

    database.add_to_metadata(os.getcwd() + f'/{table}/metadata.json', {**header, **fileResults})

    return 1

def main():
    connection = database.connect('ccldb.crc.nd.edu', 'biometrics')

    bxbox_query(connection)

    connection.close()

if __name__ == "__main__":
    main()
