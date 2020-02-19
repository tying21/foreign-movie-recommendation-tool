#!/bin/bash

INS="`seq 2013 2020`" 

for f in ${INS}; do
	mkdir ${f}
	unzip ${f}*.csv.zip
	mv ${f}*.csv.zip ${f}
	aws s3 sync ${f} s3://[bucket-name]/[file-name]
	rm -rf ${f}
done
