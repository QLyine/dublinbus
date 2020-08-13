# README

### Pre-Requistes on the machine that will run

  * Have docker installed
  * Have Java-8
  * Run build_jars.sh

### Install database

  Run start_database_docker.sh script presented on the root folder of this
  project

### Instructions on how to install and/or access to the database;

#### Access 

  docker exec -it aerospike bash

#### Show sets

  aql -c "show sets"

#### Query Table

  aql -c "select * from test.[set]"

  Ex: aql -T 10000000 -c "select * from test.vehicles"

###  A data loader script

  Run load_dataset.sh script file. It took one minute on my machine to load
  each csv file.

### Instructions on how to launch the HTTP service

  Run run_api.sh .

  It will launch an http server on port 8080 with swagger. 

  To access swagger :

  http://localhost:8080/v1/openap

#### Examples

  * Given a time frame [start-time, end-time] and an Operator, what is the list of vehicle IDs?

  curl -X GET "http://localhost:8080/v1/operator/1352160000000000/1352246396000000/CD/vehicles?stopped=false" -H  "accept: application/json"

  Which is the same as 

  curl -X GET "http://localhost:8080/v1/operator/1352160000000000/1352246396000000/CD/vehicles" -H  "accept: application/json"

  *  Given a time frame [start-time, end-time] and a fleet, which vehicles are at a stop?
  
  curl -X GET \
  "http://localhost:8080/v1/operator/1352160002000000/1352160002000000/SL/vehicles?stopped=true" -H  "accept: application/json"
