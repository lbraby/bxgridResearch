#!/usr/bin/env python3

import mysql.connector

def main():
    usernameInput = input("Enter username: ")
    passwordInput = input("Enter password: ")
    connection = mysql.connector.connect(user=usernameInput, password=passwordInput, host='ccldb.crc.nd.edu', database='biometrics')
    cursor = connection.cursor()
    cursor.execute("Show tables;")
    for line in cursor.fetchall():
        print(line)

    connection.close()

if __name__ == '__main__':
    main()
