#!/bin/bash

java -server -cp commons-codec-1.2.jar:commons-httpclient-3.1.jar:commons-logging-1.0.4.jar:jinterface-1.5.3.2.jar:junit-3.8.1.jar:riak-client-0.9.1.jar:riak-java-client-jinterface-node-1.0.jar com.basho.riak.jinterface.App $@
