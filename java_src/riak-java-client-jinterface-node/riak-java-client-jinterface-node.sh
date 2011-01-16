#!/bin/bash

[ -d `pwd`/logs ] || mkdir `pwd`/logs
java -server -cp lib:lib/*:target/riak-java-client-jinterface-node-1.0-SNAPSHOT.jar com.basho.riak.jinterface.App $@
