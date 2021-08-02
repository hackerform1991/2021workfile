#!/bin/bash

. ~/.profile 

# script path /oracle/ma-data/
# 数仓
dwip=82.211.15.106  
# 管会
maip=82.211.15.106

 # get the useful date values

if [ $# != 1 ]; then
  echo Using system date as parameter.
  thisdate=`date "+%Y-%m-%d"`
else
  thisdate=`echo $1`
fi

# insert log
req_id=`sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on
select request_id_s.nextval from dual;
quit;
EOF`

req_id=`echo $req_id`

sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on

execute sys_req_process_pck.start_log('$req_id', 'ETL script', 'SYS Auto', 'MA ETL','0','startMaEtl_Wind_onDate.sh $thisdate');
quit;

EOF


# updatedate 
  sqlplus etl_control/oracle@$dwip:1521/ICBCAMCDW<<EOF
  
  execute etl_control_std_pkg.set_process_date('$thisdate');

EOF


# stdtoext 
  sqlplus etl_control/oracle@$dwip:1521/ICBCAMCDW<<EOF
  
  execute etl_control_ext_pkg.start_daily_process;

EOF

# 将仓库数据导出

cd /oracle/ma-data

path="/oracle/ma-data"

T=`date +%D:%r`:
echo $T


rm /oracle/ma-data/$thisdate-WIND-DATA.dmp

exp ext_layer/oracle@$dwip:1521/ICBCAMCDW file=/oracle/ma-data/$thisdate-WIND-DATA.dmp tables="(MA_SHIBORPRICES,MA_CBONDCURVECNBD)"


# 将导出的数据导入到管会中


  # TRUNCATE table
  sqlplus maetl/maetl@$maip:1521/masdb<<EOF
TRUNCATE TABLE MA_SHIBORPRICES;
TRUNCATE TABLE MA_CBONDCURVECNBD;
exit;
EOF


if [ -f $thisdate-WIND-DATA.dmp ];then 

  imp maetl/maetl@$maip:1521/masdb file=/oracle/ma-data/$thisdate-WIND-DATA.dmp tables="(MA_SHIBORPRICES,MA_CBONDCURVECNBD)" fromuser=ext_layer touser=MAETL ignore=y>>error.log

echo "执行maadmin层存储过程"
sqlplus maadmin/maadmin@$maip:1521/masdb<<EOF

  -- Call the procedure
   execute etl_to_ma_process_pck.wind_data_daily;

EOF


  else 
  echo "file is not exist on the parm date"
fi



sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on

execute sys_req_process_pck.end_log('$req_id', 'END - NORMAL', '', '0', 'startMaEtl_Wind_onDate.sh $thisdate');
quit;

EOF



