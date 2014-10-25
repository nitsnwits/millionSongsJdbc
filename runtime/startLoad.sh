#!/bin/bash

echo "Starting load process.."
java -Xmx1500m -cp /Users/neerajsharma/Downloads/json-simple-1.1.1.jar:/Users/neerajsharma/Downloads/slf4j-1.7.7/slf4j-api-1.7.7.jar:/Users/neerajsharma/Downloads/slf4j-1.7.7/slf4j-simple-1.7.7.jar:/Users/neerajsharma/Downloads/postgresql-9.3-1102.jdbc3.jar:.:./bin:./src:../config/schema.conf:../config/server.conf LoadData
