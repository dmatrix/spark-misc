#!/bin/bash
# check if a directory exists then remove it
if [ -d "spark-warehouse" ]; then
    rm -rf spark-warehouse
    fi                  
spark-pipelines run --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=spark-warehouse 
    