#!/bin/bash

# exit when any command fails
set -e

mvn clean -Pspark-3.4 -Pscala-2.12
mvn test -Pspark-3.4 -Pscala-2.12

mvn clean -Pspark-3.5 -Pscala-2.12
mvn test -Pspark-3.5 -Pscala-2.12


mvn clean -Pspark-3.4 -Pscala-2.13
mvn test -Pspark-3.4 -Pscala-2.13

mvn clean -Pspark-3.5 -Pscala-2.13
mvn test -Pspark-3.5 -Pscala-2.13
