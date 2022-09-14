#!/usr/bin/env python3

import re
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
    targetIds = ['234334', '234335', '234336', '234337', '234432', '234433', '234434', '234435', '234436', '234437']
    targetIdsResults = {}

    connection = MySQL_connect('ccldb.crc.nd.edu', 'biometrics')

    # query files and replicase tables for each id in targetIds
    for fileid in targetIds:
        filesResults = MySQL_query(f'SELECT * from files where fileid={fileid}', connection)
        replicasResults = MySQL_query(f'SELECT * from replicas where fileid={fileid}', connection)

        targetIdsResults[fileid] = {**filesResults, **replicasResults}


    #idResults = {**filesResults, **replicasResults}

    with open("sample.json", "w") as outfile:
        outfile.write(json.dumps(targetIdsResults, indent=4, default=str))

    connection.close()

if __name__ == '__main__':
    main()
