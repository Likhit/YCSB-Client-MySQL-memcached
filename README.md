# YCSB-Client-MySQL-memcached
This is the YCSB client that uses a cache augmented architecture consisting of MySQL and memcached. The source YCSB repository is here. https://github.com/brianfrankcooper/YCSB/

## How to run YCSB
Install the latest memcached from https://memcached.org/ and MySQL database https://dev.mysql.com/downloads/mysql/5.6.html#downloads. 

The main class is com.yahoo.ycsb.db.JdbcDBClientSelector.
db.properties file contains MySQL and memcached configurations.

### Build YCSB
```
mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests
```
### Load MySQL database
Start MySQL database.
```
bin/ycsb load jdbc -P workloads/workloadb -P db.properties
```
### Run YCSB
Benchmark will run with memcached if 'use_cache' is true. Otherwise, it runs against MySQL. Make sure to start memcached if you set 'use_cache' to true. 
```
bin/ycsb run jdbc -P workloads/workloadb -P db.properties -s -threads 1 -p use_cache=true
```
