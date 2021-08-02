# This program is used for daily auto program calling.

#! /bin/bash
. ~/.profile

uti_path=/oracle/ma_utility
PWD=`pwd`

cd $uti_path
echo Start time: `date`
# run batch to make pricing IRC
./execute_batch.sh 0 63

# put other scripts for daily auto running



cd $PWD
echo End time: `date`
