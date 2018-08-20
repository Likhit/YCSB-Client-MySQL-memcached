#!/bin/bash
cd ~/Documents/PhdUSC/IQ-WhalinTwemcache && mvn clean && mvn install -DskipTests
cd ~/Documents/PhdUSC/NVCacheCoordinator/ && bash build.sh
cd ~/Documents/PhdUSC/NVM-Recovery/nvcache/ && mvn clean && mvn install -DskipTests
cd ~/Documents/PhdUSC/CADS_YCSB/ && mvn -pl com.yahoo.ycsb:nvcache-binding -am clean package -DskipTests