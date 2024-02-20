#!/bin/bash

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

cd $GLUTEN_DIR
mvn clean package -Pbackends-velox -Prss -Pspark-3.2 -Piceberg -DskipTests  -Dcheckstyle.skip=true -DskipScalastyle=true
