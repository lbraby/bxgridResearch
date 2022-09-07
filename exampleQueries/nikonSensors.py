#!/usr/bin/env python3

import mysql.connector
import csv

def main():
    usernameInput = input("Enter username: ")
    passwordInput = input("Enter password: ")
    connection = mysql.connector.connect(user=usernameInput, password=passwordInput, host='ccldb.crc.nd.edu', database='biometrics')
    cursor = connection.cursor()
    cursor.execute('''SELECT sensorid, name, model, spectrum, resolution FROM sensors
                      WHERE name LIKE '%Nikon%';''')
    # write result to csv
    with open('nikonSensors.csv', 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['sensorid', 'name', 'model', 'spectrum', 'resolution'])
        for line in cursor.fetchall():
            print(line)
            csv_out.writerow(line)

    connection.close()

if __name__ == '__main__':
    main()
