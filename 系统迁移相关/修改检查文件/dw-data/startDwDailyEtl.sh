#!/bin/bash 


. ~/.profile 
#script path /dw-data/

 # get the useful date values
thisresult=`sqlplus etl_control/oracle@82.211.13.98:1521/ICBCAMCDW<<EOF
set feedback off
set colsep
set heading off
set trimout on 
set serveroutput off
set echo off
set pagesize 0
set define off
set serveroutput off;
select '[thisresultpositionstart]'||to_char(data_date,'yyyyMMdd')||'[thisresultpositionend]' from ctl_general_parameter;
EOF`
 

echo $thisresult
tmp=${thisresult#*'[thisresultpositionstart]'}
echo $tmp
thisdate=${tmp%'[thisresultpositionend]'*}
echo $thisdate



echo "执行时间$thisdate"

echo "执行src层数据抽取"
echo "开始执行ebsreport"
cd /dw-data/EBS-REPORT
./startreportetl.sh $thisdate


echo "开始执行ebsreportaud"
cd /dw-data/EBS-REPORT-AUD
./startreportaudetlnew.sh $thisdate

echo "开始执行ebsdata"
cd /dw-data/EBS-DATA
./start_etl.sh $thisdate

echo "开始执行aics"
cd /dw-data/AICS
./start_etl.sh $thisdate


echo "执行std层存储过程"
sqlplus etl_control/oracle@82.211.13.98:1521/ICBCAMCDW<<EOF

 execute etl_control_std_pkg.start_daily_process;

exit;
EOF

echo "setdate+1"
sqlplus etl_control/oracle@82.211.13.98:1521/ICBCAMCDW<<EOF

 execute etl_control_std_pkg.set_process_date_add_1;

exit;
EOF