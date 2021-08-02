#!/bin/bash

#script path /dw-data
. ~/.profile

dwip=82.211.13.98 

# updatedate 
  sqlplus etl_control/oracle@$dwip:1521/ICBCAMCDW<<EOF
  
  execute etl_control_std_pkg.set_import_date('$1');

EOF

cd /dw-data
. ./startDwDailyEtl.sh