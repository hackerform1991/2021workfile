#!/bin/bash

#script path /oracle/dw-data
. ~/.profile

dwip=82.211.15.106 
maip=82.211.15.106


# insert log
req_id=`sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on
select request_id_s.nextval from dual;
quit;
EOF`

req_id=`echo $req_id`

sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on

execute sys_req_process_pck.start_log('$req_id', 'ETL script', 'SYS Auto', 'DW daily','0','startDwEtlOnDate.sh $1');
quit;

EOF

# updatedate 
  sqlplus -s etl_control/oracle@$dwip:1521/ICBCAMCDW<<EOF
  
  execute etl_control_std_pkg.set_import_date('$1');

EOF


sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on

execute sys_req_process_pck.start_log('$req_id', 'ETL script'

quit;
EOF


cd /oracle/dw-data
. ./startDwDailyEtl.sh

sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on

execute sys_req_process_pck.end_log('$req_id', 'END - NORMAL', '', '0', 'startDwEtlOnDate.sh $1');
quit;

EOF
