#!/bin/sh

if [ $# -ne 2 ]; then
    echo 'usage: insertSchemaInfo.sh <schema name> <path to schema file>';
    exit 1;
fi

/usr/bin/curl http://localhost:8080/solbase/schema/$1 --data-binary @$2 -H 'Content-type:text/xml; charset=utf-8';
