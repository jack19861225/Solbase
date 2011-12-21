#!/bin/sh


echo "create 'SI', 'info', {NAME=>'info',REPLICATION_SCOPE=>1,VERSION=>1}" | hbase shell
echo "create 'Docs', 'field', 'allTerms', 'timestamp', {COMPRESSION=>'none',NAME=>'field',VERSION=>1,REPLICATION_SCOPE=>1},{COMPRESSION=>'none',NAME=>'allTerms',VERSION=>1,REPLICATION_SCOPE=>1},{COMPRESSION=>'none',NAME=>'timestamp',VERSION=>1, REPLICATION_SCOPE=>1}" | hbase shell
echo "create 'TV', 'd', {COMPRESSION=>'none',NAME=>'d',VERSION=>1, REPLICATION_SCOPE=>1}" | hbase shell
echo "create 'DocKeyIdMap', 'docId',{COMPRESSION=>'none',NAME=>'docId',VERSION=>1,REPLICATION_SCOPE=>1}" | hbase shell
echo "create 'Sequence', 'id',{COMPRESSION=>'none',NAME=>'id',VERSION=>1,REPLICATION_SCOPE=>1}" | hbase shell
echo "create 'TVVersionId', 'timestamp', {COMPRESSION=>'none',NAME=>'timestamp',VERSION=>1,REPLICATION_SCOPE=>1}" | hbase shell
echo "create 'uniq_checksum_user_media', 'userMediaKey', {COMPRESSION=>'none',NAME=>'userMediaKey',VERSION=>1,REPLICATION_SCOPE=>1}" | hbase shell
