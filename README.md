# bxgridResearch
Fall 2022 Research work for Notre Dame's [Biometrics Research Grid](https://bxgrid.cse.nd.edu/browse.php)
## Progress by Week
### Week 1
- set up miniconda
  - python 3.7
  - [mysql-python-connector](https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html)
- wrote 3 example queries to increase comfortability with my-python-connector
  - silentGeneration.py: select all users born before 1945
  - subjectOnDay.py: select all face images of subject on set day
  - nikonSensors.py: select all nikon sensors
### Week 2
- metadata
  - query files and replicas tables for 10 fileids using mysql-python-connector
  - convert tuple results to dictionary and store in [chirpedFiles.json](https://github.com/lbraby/bxgridResearch/blob/main/Week02_chirpingFiles/chirpedFiles.json)
- files
  - [chirp](https://cctools.readthedocs.io/en/stable/chirp/) each file from a replica machine
  - validate chirped file by computing md5sum and comparing with checksum in query results
### Week 3
- chirp files into directory following table/subjectid/date directory schema
  - each file's corresponding query results are stored in directory's results.json
- iterate through replicas until successful chirp
  - in previous week, only attempted chirp on one host
### Week 4
- increased number of files chirped into treed directories
- restructured code from week 3
- ran performance tests on chirping
### Week 5
- create preliminary version of "bxgrid in a box" (bxbox)
  - allow user to query files from specified bxgrid table into filesystem with desired schema
  - bxbox syntax: "materialize {tablename} as {schema for filesystem} {MySQL WHERE, ORDER BY, LIMIT clauses}
  
- example results from bgbox materialization query in 'irises_still/'
  - bgbox> materialize irises_still as date/weather/eye where subjectid = 'nd1S04473'
