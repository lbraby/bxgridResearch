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
### Week 6
- convert preliminary materializer to take in command line arguments
- new query features:
  - user may materialize into an already existing directory if the new and old schemas match by using the '-force' flag
  - only get metadata into filesystem by using the '-dryrun' flag
  - can specify root directory, otherwise name of top level directory follows TABLE + timestamp convention
- file changes:
  - queried files may never be overwritten (even when using '-force' flag)
  - large metadata file at top level includes subject data
  - all files and directories created with mode 444
### Week 7
- change '-dryrun' flag to '-nofiles' 
- improve function usage output
- store user login credentials in $HOME/.bxgrid/credentials
- store query history in $HOME/.bxgrid/history.json
- show progress bar for materializations
- provide user with feedback on chirps
  - Warning for failed chirp
  - Error when all chirps fail
  - push failed servers to back of queue for future chirps
### Week 8
- materialization history
  - entries include command line input
  - save last materialization query in latest_materializtion.json
  - save 500 (like GNU history) last materialization queries in history_materializations.json
- SQL work (for group feature)
  - wrote general query to select n entries from each group set by user
  - work in [querywork.txt](https://github.com/lbraby/bxgridResearch/blob/main/Week08/querywork.txt)
- after meeting, approach for next week is 2-phase materialization
  - fist phase is getting data (mandatory)
  - second phase is file retrieval into directory tree (optional)
