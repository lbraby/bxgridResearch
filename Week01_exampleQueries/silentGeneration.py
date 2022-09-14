#!/usr/bin/env python3

import mysql.connector
import csv

def main():
    usernameInput = input("Enter username: ")
    passwordInput = input("Enter password: ")
    connection = mysql.connector.connect(user=usernameInput, password=passwordInput, host='ccldb.crc.nd.edu', database='biometrics')
    cursor = connection.cursor()
    cursor.execute('''SELECT subjectid, gender, YOB, race FROM subjects
                   WHERE YOB < 1945
                   ORDER BY YOB''')
    # write result to csv
    with open('silentGeneration.csv', 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['subjectid', 'gender', 'YOB', 'race'])
        for line in cursor.fetchall():
            print(line)
            csv_out.writerow(line)

    connection.close()

if __name__ == '__main__':
    main()
