#!/bin/bash

. ~/.profile 

# script path /oracle/ma-data/
# 数仓
dwip=82.211.15.106  
# 管会
maip=82.211.15.106

 # get the useful date values
thisresult=`sqlplus maadmin/maadmin@$maip:1521/masdb<<EOF
set feedback off
set colsep
set heading off
set trimout on 
set serveroutput off
set echo off
set pagesize 0
set define off
set serveroutput off;
select '[thisresultpositionstart]'||to_char(data_date,'yyyyMMdd')||'[thisresultpositionend]' from p_general_parameter;
EOF`
 
 
echo $thisresult
tmp=${thisresult#*'[thisresultpositionstart]'}
echo $tmp
thisdate=${tmp%'[thisresultpositionend]'*}
echo $thisdate


# updatedate 
  sqlplus etl_control/oracle@$dwip:1521/ICBCAMCDW<<EOF
  
  execute etl_control_std_pkg.set_process_date('$thisdate');

EOF

# insert log
req_id=`sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on
select request_id_s.nextval from dual;
quit;
EOF`

req_id=`echo $req_id`

sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on

execute sys_req_process_pck.start_log('$req_id', 'ETL script', 'SYS Auto', 'MA ETL','0','startMaEtl.sh $thisdate');
quit;

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


rm /oracle/ma-data/$thisdate-MA-DATA.dmp

exp ext_layer/oracle@$dwip:1521/ICBCAMCDW file=/oracle/ma-data/$thisdate-MA-DATA.dmp tables="(MA_BSSHEET_HEADER,MA_PLSHEET_HEADER,MA_EXPDETAIL_HEADER,MA_BSSHEET_LINE,MA_PLSHEET_LINE,MA_EXPDETAIL_LINE,MA_AP_INVOICES_ALL
,MA_AP_INVOICE_LINES_ALL,MA_AP_INVOICE_DISTRIBUTIONS_ALL,MA_AP_SUPPLIERS,MA_GL_LEDGERS,MA_FND_FLEX_VALUE_SETS,MA_FND_FLEX_VALUES,MA_GL_CODE_COMBINATIONS,MA_GL_BALANCES,MA_FND_ID_FLEX_SEGMENTS,MA_BUSINESS_ACCOUNT_INFOR,MA_PROJECT,MA_TP_CASH_FLOW,MA_CUSTOMER_INFO,MA_CUSTOMERINFO,MA_SHIBORPRICES,MA_CBONDCURVECNBD,MA_CGBBENCHMARK,MA_PER_ALL_PEOPLE_F)"


# 将导出的数据导入到管会中


  # TRUNCATE table
  sqlplus maetl/maetl@$maip:1521/masdb<<EOF
TRUNCATE table  MA_BSSHEET_HEADER;
TRUNCATE table  MA_PLSHEET_HEADER;
TRUNCATE table  MA_EXPDETAIL_HEADER;
TRUNCATE table  MA_BSSHEET_LINE;
TRUNCATE table  MA_PLSHEET_LINE;
TRUNCATE table  MA_EXPDETAIL_LINE;
TRUNCATE table  MA_AP_INVOICES_ALL;
TRUNCATE table  MA_AP_INVOICE_LINES_ALL;
TRUNCATE table  MA_AP_INVOICE_DISTRIBUTIONS_ALL;
TRUNCATE table  MA_AP_SUPPLIERS;
TRUNCATE table  MA_GL_LEDGERS;
TRUNCATE table  MA_FND_FLEX_VALUE_SETS;
TRUNCATE table  MA_FND_FLEX_VALUES;
TRUNCATE table  MA_GL_CODE_COMBINATIONS;
TRUNCATE table  MA_GL_BALANCES;
TRUNCATE table  MA_FND_ID_FLEX_SEGMENTS;
TRUNCATE table  MA_BUSINESS_ACCOUNT_INFOR;
TRUNCATE TABLE  MA_PROJECT;
TRUNCATE TABLE MA_TP_CASH_FLOW;
TRUNCATE TABLE MA_CUSTOMER_INFO;
TRUNCATE TABLE  MA_CUSTOMERINFO;
TRUNCATE TABLE MA_SHIBORPRICES;
TRUNCATE TABLE MA_CBONDCURVECNBD;
TRUNCATE TABLE MA_CGBBENCHMARK;
TRUNCATE TABLE MA_PER_ALL_PEOPLE_F;
exit;
EOF


if [ -f $thisdate-MA-DATA.dmp ];then 

  imp maetl/maetl@$maip:1521/masdb file=/oracle/ma-data/$thisdate-MA-DATA.dmp tables="(MA_BSSHEET_HEADER,MA_PLSHEET_HEADER,MA_EXPDETAIL_HEADER,MA_BSSHEET_LINE,MA_PLSHEET_LINE,MA_EXPDETAIL_LINE,MA_AP_INVOICES_ALL
,MA_AP_INVOICE_LINES_ALL,MA_AP_INVOICE_DISTRIBUTIONS_ALL,MA_AP_SUPPLIERS,MA_GL_LEDGERS,MA_FND_FLEX_VALUE_SETS,MA_FND_FLEX_VALUES,MA_GL_CODE_COMBINATIONS,MA_GL_BALANCES,MA_FND_ID_FLEX_SEGMENTS,MA_BUSINESS_ACCOUNT_INFOR,MA_PROJECT,MA_TP_CASH_FLOW,MA_CUSTOMER_INFO,MA_CUSTOMERINFO,MA_SHIBORPRICES,MA_CBONDCURVECNBD,MA_CGBBENCHMARK,MA_PER_ALL_PEOPLE_F)" fromuser=ext_layer touser=MAETL ignore=y>>error.log

echo "执行maadmin层存储过程"
sqlplus maadmin/maadmin@$maip:1521/masdb<<EOF

  -- Call the procedure
   execute etl_to_ma_process_pck.ma_total_control;

EOF

echo "执行maadmin层存储过程"
sqlplus maadmin/maadmin@$maip:1521/masdb<<EOF

  -- Call the procedure
  execute etl_data_process_pck.total_control;

EOF

#echo "更新管会的当前数据日期"
#sqlplus maadmin/maadmin@$maip:1521/masdb<<EOF

 # -- Call the procedure
 # execute etl_data_process_pck.update_p_general_parameter;

#EOF

  else 
  echo "file is not exist on the parm date"
fi



sqlplus -s maadmin/maadmin@$maip:1521/masdb<<EOF
set pagesize 0 heading off echo off termout off feedback off linesize 1200 colsep "," trimspool on trimout on

execute sys_req_process_pck.end_log('$req_id', 'END - NORMAL', '', '0', 'startMaEtl.sh $thisdate');
quit;

EOF


