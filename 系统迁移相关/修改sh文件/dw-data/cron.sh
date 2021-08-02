#!/bin/bash 


. ~/.profile 
#script path /oracle/dw-data/

 # get the useful date values
thisresult=`sqlplus etl_control/oracle@82.211.15.106:1521/ICBCAMCDW<<EOF
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

echo "开始数据仓库日处理"
cd /oracle/dw-data/
./startDwEtlOnDate.sh $thisdate

#echo "提取数据，并加工至管会"
#cd /oracle/ma-data/
#./startMaEtl.sh