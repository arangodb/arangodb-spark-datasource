#!/bin/bash

mvn clean -Pspark-3.4 -Pscala-2.12
mvn clean -Pspark-3.4 -Pscala-2.13
mvn clean -Pspark-3.5 -Pscala-2.12
mvn clean -Pspark-3.5 -Pscala-2.13
mvn clean -Pspark-4.0 -Pscala-2.13.18
mvn clean -Pspark-4.1 -Pscala-2.13.18
