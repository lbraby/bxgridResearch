#!/usr/bin/env python3

import mysql.connector
import csv

def main():
    usernameInput = input("Enter username: ")
    passwordInput = input("Enter password: ")
    connection = mysql.connector.connect(user=usernameInput, password=passwordInput, host='ccldb.crc.nd.edu', database='biometrics')
    cursor = connection.cursor()
    cursor.execute('''SELECT id, recordingid, shotid, url, date, emotion, weather from faces_still
                      WHERE subjectid='nd1S04673' AND date='2003-04-15 13:00:00';''')
    # write result to csv
    with open('subjectOnDay.csv', 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['id', 'recordingid', 'shotid', 'url', 'date', 'emotion', 'weather'])
        for line in cursor.fetchall():
            print(line)
            csv_out.writerow(line)

    connection.close()

if __name__ == '__main__':
    main()
