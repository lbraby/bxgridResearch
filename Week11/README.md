# Bgbox
## 1 Introduction
Bgbox (BxGrid in a box) consists of two tools (query and materialze) to help researchers acquire data in a structed way. For those already familiar with SQL and bxgrid, this tool serves to simply speed up the process of acquiring and sorting data. For those who lack this knowledge, Bgbox offers all the information you need to query, sort, and store data from Notre Dame's biometrics grid.
## 2 Query Tool
query data from bxgrid's biometrics database and store in csv file
```sh
query.py [option...] TABLE
```
### 2.1 Tables and Their Queryable Attributes
- faces_still
  - recordingid, shotid, sequenceid, url, date, format, camera, subjectid, glasses, source1, emotion, source2, rank, lefteye, righteye, nose, mouth, yaw, pitch, stageid, weather, collectionid, environmentid, sensorid, illuminantid, illuminantid1, illuminantid2, state, fileid, by_user, date_added, added_by, comment, temp_collectionid, enrolled_date
- faces_mov
  - recordingid, shotid, sequenceid, url, date, format, camera, subjectid, glasses, source1, emotion, source2, rank, talking, action, head, stageid, weather, collectionid, environmentid, sensorid, illuminantid, illuminantid1, fileid, state, by_user, date_added, added_by, temp_collectionid, enrolled_date
- faces_3d
  - recordingid, shotid, sequenceid, url, date, format, subjectid, glasses, source1, emotion, source2, stageid, weather, collectionid, environmentid, sensorid, illuminantid, illuminantid1, illuminantid2, fileid, state, by_user, date_added, added_by, temp_collectionid, enrolled_date, yaw
- irises_still
  - recordingid, shotid, sequenceid, url, date, format, camera, subjectid, eye, color, pose, motion, treatment, coordinate00, coordinate01, coordinate02, coordinate03, coordinate04, coordinate05, coordinate06, coordinate07, coordinate08, coordinate09, coordinate10, coordinate11, coordinate12, coordinate13, coordinate14, coordinate15, coordinate16, glasses, collectionid, stageid, environmentid, sensorid, illuminantid, illuminantid1, weather, shot, state, fileid, by_user, date_added, added_by, comment, temp_collectionid, enrolled_date, contacts, contacts_type, contacts_texture, contacts_toric, contacts_cosmetic 
- irises_mov
  - recordingid, shotid, sequenceid, url, date, format, camera, subjectid, glasses, source1, eye, color, pose, motion, treatment, conditions, stageid, weather, collectionid, environmentid, sensorid, illuminantid, illuminantid1, fileid, state, by_user, date_added, added_by, temp_collectionid, enrolled_date, contacts, contacts_type, contacts_texture, contacts_toric, contacts_cosmetic
### 2.2 Other Queryable Attributes
- files
  - fileid, userkey, checksum, extension, size, lastcheck, rank, fstate
- subjects
  - subjectid, gender, source1, YOB, source2, race, source3, sequence, date_added, s_irises, ethnicity
### 2.3 Command Line Options
- -limit N
  - N must be a valid numeric or 'unlimited' to get all entries
  - required unless using a custom MySQL query  
- -outcsv FILENAME
  - specify output file for query tool
  - if flag not passed, FILENAME = query_{{timestamp}}.csv  
- -mysql QUERY
  - use a custom MySQL query
  - removes the need to specify a TABLE  
- -where CONDITIONS
  - limit query to files fulfilling conditions
  - CONDITIONS must be written in MySQL WHERE clause syntax  
- -schema ATTRIBUTES
  - select attributes to query
  - ATTRIBUTES must follow form attr1/attr2/...
  - attributes may come from TABLE or files/subjects tables
  - all attributes selected if no flag passed  
- -overwrite
  - if output file already exists, overwrite it  
- -credentials
  - do not use previously saved user credentials to access bxgrid  
### 2.4 Example Queries
#### get select attributes from table with condition:
```sh
./query.py -limit 50 -where "color = 'blue'" -schema subjectid/fileid/date -outcsv exampleQuery1.csv irises_mov
```
#### using query tool with custom MySQL query:
```sh
./query.py -mysql "select * from subjects inner join faces_3d where gender = 'Female' limit 300" -outcsv exampleQuery2.csv
```
#### get all entries from table:
```sh
./query.py -limit unlimited -outcsv exampleQuery3.csv faces_still
```
## 3 Materialize tool
materialize data from provided csv into filesystem
```sh
materialize.py [option...] CSVFILE
```
### 3.1 Command Line Options
- -headdir DIR
  - set name of top level directory
  - if flag not passed, DIR = materialization_{timestamp}
- -schema SCHEME
  - set schema for directory tree
  - SCHEME must follow form attr1/attr2/...
  - all attr's must be columns in CSVFILE
- -nofiles
  - run materialization without downloading files
  - only metadata files and directory tree created
- -smartdownload
  - allow materializer to use local files when available
  - enabling smart download may increase the speed of execution
- -force
  - allow materialization into existing directory as long as scemas match
- -resume
  - resume previous execution
  - already downloaded files will not be overwritten
- -fileid ATTR
  - specify which field contains fileids for materialization
  - if flag not passed, ATTR = fileid
### 3.2 Example Materializations
materialize directory tree and metadata file without media
```sh
./materialize.py -headdir exampleMaterialization1 -schema subjectid/date -nofiles exampleQuery1.csv
```
````
exampleMaterialization1/
????????? metadata.json
????????? nd1S04236
???   ????????? 03_13_2008
???       ????????? metadata_refined.json
????????? nd1S05491
    ????????? 03_26_2008
        ????????? metadata_refined.json
````
#### materialize files:
```sh
./materialize.py -headdir exampleMaterialization2 -schema subjectid/date exampleQuery1.csv
```
````
exampleMaterialization2
????????? metadata.json
????????? nd1S04236
???   ????????? 03_13_2008
???       ????????? 246048.mp4
???       ????????? 246049.mp4
???       ????????? metadata_refined.json
????????? nd1S05491
    ????????? 03_26_2008
        ????????? 247341.mp4
        ????????? 247342.mp4
        ????????? metadata_refined.json
````
#### materialize files faster if on system:
```sh
./materialize.py -headdir exampleMaterialization2 -schema subjectid/date -smartdownload exampleQuery1.csv
```
