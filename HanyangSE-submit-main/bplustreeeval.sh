#!/usr/bin/bash

rm -f externalsorteval.log

mvn clean compile 1> /dev/null

# DO NOT CHANGE MAVEN_OPTS
MAVEN_OPTS=-Xmx16m mvn exec:java -Dexec.mainClass="edu.hanyang.test.BPlusTreeEval" 2>> bplustreeeval.log
