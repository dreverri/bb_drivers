#!/bin/bash

java -server -cp lib:lib/* com.basho.riak.jinterface.App $@
