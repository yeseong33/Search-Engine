#!/usr/bin/bash

nblocks="100 200 500 1000"
bsizes="8192 16384"

rm -f externalsorteval.log

# DO NOT CHANGE MAVEN_OPTS
for n in $nblocks; do
  for s in $bsizes; do
    MAVEN_OPTS=-Xmx16m mvn exec:java -Dexec.mainClass="edu.hanyang.test.ExternalSortEval" -Dexec.args="$s $n" 2>> externalsorteval.log 1>/dev/null
  done
done
