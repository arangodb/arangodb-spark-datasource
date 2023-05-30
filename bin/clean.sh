#!/bin/bash

mvn clean -Pspark-2.4 -Pscala-2.11
mvn clean -Pspark-2.4 -Pscala-2.12
mvn clean -Pspark-3.1 -Pscala-2.12
mvn clean -Pspark-3.2 -Pscala-2.12
mvn clean -Pspark-3.2 -Pscala-2.13
mvn clean -Pspark-3.3 -Pscala-2.12
mvn clean -Pspark-3.3 -Pscala-2.13
