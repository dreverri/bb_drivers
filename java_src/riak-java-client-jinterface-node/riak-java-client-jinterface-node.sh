#!/bin/bash

java -server -cp ./target/dependency/*:./target/* com.basho.riak.jinterface.App $@
