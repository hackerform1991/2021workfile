#!/bin/bash

. ~/.profile 

# script path /ma-data/
# 数仓
dwip=82.211.13.98  
# 管会
maip=82.211.13.98
 # get the useful date values
thisresult=`sqlplus maadmin/maadmin@$maip:1521/madb<<EOF
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

# stdtoext 
  sqlplus etl_control/oracle@$dwip:1521/ICBCAMCDW<<EOF
  
  execute etl_control_ext_pkg.start_daily_process;

EOF

# 将仓库数据导出

cd /ma-data

path="/ma-data"

T=`date +%D:%r`:
echo $T


rm /ma-data/$thisdate-MA-DATA.dmp

exp ext_layer/oracle@$dwip:1521/ICBCAMCDW file=/ma-data/$thisdate-MA-DATA.dmp tables="(MA_BSSHEET_HEADER,MA_PLSHEET_HEADER,MA_EXPDETAIL_HEADER,MA_BSSHEET_LINE,MA_PLSHEET_LINE,MA_EXPDETAIL_LINE,MA_AP_INVOICES_ALL
,MA_AP_INVOICE_LINES_ALL,MA_AP_INVOICE_DISTRIBUTIONS_ALL,MA_AP_SUPPLIERS,MA_GL_LEDGERS,MA_FND_FLEX_VALUE_SETS,MA_FND_FLEX_VALUES,MA_GL_CODE_COMBINATIONS,MA_GL_BALANCES,MA_FND_ID_FLEX_SEGMENTS,MA_BUSINESS_ACCOUNT_INFOR,MA_PROJECT,MA_TP_CASH_FLOW)"


# 将导出的数据导入到管会中


  # TRUNCATE table
  sqlplus maetl/maetl@$maip:1521/madb<<EOF
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
exit;
EOF


if [ -f $thisdate-MA-DATA.dmp ];then 

  imp maetl/maetl@$maip:1521/madb file=/ma-data/$thisdate-MA-DATA.dmp tables="(MA_BSSHEET_HEADER,MA_PLSHEET_HEADER,MA_EXPDETAIL_HEADER,MA_BSSHEET_LINE,MA_PLSHEET_LINE,MA_EXPDETAIL_LINE,MA_AP_INVOICES_ALL
,MA_AP_INVOICE_LINES_ALL,MA_AP_INVOICE_DISTRIBUTIONS_ALL,MA_AP_SUPPLIERS,MA_GL_LEDGERS,MA_FND_FLEX_VALUE_SETS,MA_FND_FLEX_VALUES,MA_GL_CODE_COMBINATIONS,MA_GL_BALANCES,MA_FND_ID_FLEX_SEGMENTS,MA_BUSINESS_ACCOUNT_INFOR,MA_PROJECT,MA_TP_CASH_FLOW)" fromuser=ext_layer touser=MAETL ignore=y>>error.log

echo "执行maadmin层存储过程"
sqlplus maadmin/maadmin@$maip:1521/madb<<EOF

  -- Call the procedure
   execute etl_to_ma_process_pck.ma_total_control;

EOF

echo "执行maadmin层存储过程"
sqlplus maadmin/maadmin@$maip:1521/madb<<EOF

  -- Call the procedure
  execute etl_data_process_pck.total_control;

EOF


  else 
  echo "file is not exist on the parm date"
fi





