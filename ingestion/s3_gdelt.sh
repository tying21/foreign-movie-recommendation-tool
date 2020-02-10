{\rtf1\ansi\ansicpg1252\cocoartf2511
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fmodern\fcharset0 CourierNewPSMT;\f1\fnil\fcharset0 HelveticaNeue;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;\red255\green255\blue255;}
{\*\expandedcolortbl;;\cssrgb\c0\c0\c0;\cssrgb\c100000\c100000\c100000;}
\margl1440\margr1440\vieww17280\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\sl320\partightenfactor0

\f0\fs28 \cf2 \cb3 \expnd0\expndtw0\kerning0
#!/bin/bash\
\
INS="`seq 2013 2020`" \
\
for f in $\{INS\}; do\
	mkdir $\{f\}\
	unzip $\{f\}*.csv.zip\
	mv $\{f\}*.csv.zip $\{f\}\
	aws s3 sync $\{f\} 
\f1\fs24 \cf0 \cb1 \kerning1\expnd0\expndtw0 s3://foreign-movie-rec/gdelt
\f0\fs28 \cf2 \cb3 \expnd0\expndtw0\kerning0
\
	rm -rf $\{f\}\
done}