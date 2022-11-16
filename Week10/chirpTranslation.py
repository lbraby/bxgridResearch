#!/usr/bin/env python3

import Chirp
import mysql.connector

def get_replicas(numFiles: int):
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
        return []

    cursor = connection.cursor()

connections = {}
# how to know whether file isn't there or if connection is lost
  # no way to tell (will get couldn't get file error)
  # ben is going to make a change to allow this error handling

# stat method: allows you to tell if file exists
def main():
    hosts = ["bxgrid.crc.nd.edu", "disc05.crc.nd.edu", "disc23.crc.nd.edu", "disc18.crc.nd.edu"]
    for host in hosts:
        try:
            client = Chirp.Client(host, authentication = ['unix'])
            connections[host] = client

        except Chirp.AuthenticationFailure as e:
            print(f"failed to connect to {host}")
    # if client.stat(server_name):
        # client.get(server_name, local_name)
    print(connections)
if __name__ == "__main__":
    main()
