#! /bin/bash
cd /dw-data/EBS-DATA
echo "strat to load data for csv document"
echo "date is " $1
DOCUMENTNAME="$1-EBS-DATA.READY"
echo "$DOCUMENTNAME will be used for etl"
T=`date +%D:%r`:
echo "NOW: $T"
# path location 
path="/dw-data/EBS-DATA"
# CONTROL FILE PATH
ctl_path="/dw-data/EBS-DATA"
echo " ">>"$path"/error.log
echo " ">>"$path"/error.log
echo "***********************">>"$path"/error.log
echo "start to etl the base data">>"$path"/error.log
echo "time: $T">>"$path"/error.log
echo "document: $1-EBS-DATA">>"$path"/error.log
echo "***********************">>"$path"/error.log

# find the document named xxxx.xx.xx.READY is exist in the folder or not 
if [ -f "$path/$DOCUMENTNAME" ];then 
  echo "$T $DOCUMENTNAME ready to load data "
  echo "[INFO] $T $DOCUMENTNAME ready to load data ">>"$path"/error.log
  
  # create new folder  when the timefolder not exist
  if [ ! -d "$path/$1" ];then 
    echo "$T $1 folder is not exist , will create the folder after"
	mkdir "$path/$1"
    mkdir "$path/$1"/csv
	mkdir "$path/$1"/log
	mkdir "$path/$1"/bad
    echo "[INFO] $T create folder named $1">>"$path"/error.log
  else
  # delete the tar file befor tar-x 
    echo "$T $1 folder is existed , will delete the folder befor tar"
    echo "[INFO] $T $1 is created will delete befor tar">>"$path"/error.log
    rm -rf "$path/$1"
	mkdir "$path/$1"
    mkdir "$path/$1"/csv
	mkdir "$path/$1"/log
	mkdir "$path/$1"/bad
    echo "[INFO] $T new a folder named $1 again">>"$path"/error.log
  fi
  
  # tar-x the time.tar to the folder time/csv 
  echo "$T tar-x to the folder named $1"
  echo "[INFO] $T tar-x the file to $1/csv">>"$path"/error.log
  if [ -f "$path/$1-EBS-DATA.tar" ];then
    echo "[INFO] $T tar -xvf start:">>"$path"/error.log
    tar -xvf "$path/$1-EBS-DATA.tar" -C "$path"/"$1"/csv >>"$path"/error.log
  else 
    echo "$T $1-EBS-DATA.tar is not existed "
    echo "[ERROR] $T $1-EBS-DATA.tar is not existed ">>"$path"/error.log
  fi
  #######ONLOADDATE#######
 


 #truncate table EBS_DATA_HEADER
 sqlplus src_layer/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
truncate table EBS_DATA_HEADER;
commit;
exit;
EOF
  
 
 for file in $path/$1/csv/$1* ;
 do  
  #get  document name and period 
  echo $file
  echo "[INFO] $T $file is begining"
  period=${file:0-10:6}
  echo $period
  echo "$T begin to load table  EBS_DATA_HEADER"
  echo "[INFO] $T begin to load table  EBS_DATA_HEADER">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$path"/control/EBS_DATA_HEADER.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$file\""" "$path"/control/EBS_DATA_HEADER.ctl 
  echo "************begin**************">>"$path"/error.log
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$path"/control/EBS_DATA_HEADER.ctl
  # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/$file.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  done
  

  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$file.csv',
'src_layer.EBS_BSSHEET_HEADER',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/$file.log',
'$path/$1/bad/$file.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log  
  
  echo  $period


  echo " ">>"$path"/error.log
 # ONLOAD TABLE EBS_GL_LEDGERS_LINE
  echo "$T begin to load table EBS_GL_LEDGERS_LINE"
  echo "[INFO] $T begin to load table EBS_GL_LEDGERS_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_GL_LEDGERS_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_gl_ledgers_$period.csv\""" "$ctl_path"/control/EBS_GL_LEDGERS_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_GL_LEDGERS_LINE.ctl bad="$path/$1"/bad/EBS_GL_LEDGERS_LINE.bad log="$path/$1"/log/EBS_GL_LEDGERS_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_GL_LEDGERS_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_gl_ledgers.csv',
'src_layer.EBS_GL_LEDGERS_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_GL_LEDGERS_LINE.log',
'$path/$1/bad/EBS_GL_LEDGERS_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  

  echo " ">>"$path"/error.log
 # ONLOAD TABLE EBS_GL_CODE_COMBINATIONS_LINE
  echo "$T begin to load table EBS_GL_CODE_COMBINATIONS_LINE"
  echo "[INFO] $T begin to load table EBS_GL_CODE_COMBINATIONS_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_GL_CODE_COMBINATIONS_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_gl_code_combinations_$period.csv\""" "$ctl_path"/control/EBS_GL_CODE_COMBINATIONS_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_GL_CODE_COMBINATIONS_LINE.ctl bad="$path/$1"/bad/EBS_GL_CODE_COMBINATIONS_LINE.bad log="$path/$1"/log/EBS_GL_CODE_COMBINATIONS_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_GL_CODE_COMBINATIONS_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_gl_code_combinations.csv',
'src_layer.EBS_GL_CODE_COMBINATIONS_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_GL_CODE_COMBINATIONS_LINE.log',
'$path/$1/bad/EBS_GL_CODE_COMBINATIONS_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  echo " ">>"$path"/error.log
 # ONLOAD TABLE EBS_GL_BALANCES_LINE
  echo "$T begin to load table EBS_GL_BALANCES_LINE"
  echo "[INFO] $T begin to load table EBS_GL_BALANCES_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_GL_BALANCES_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_gl_balances_$period.csv\""" "$ctl_path"/control/EBS_GL_BALANCES_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_GL_BALANCES_LINE.ctl bad="$path/$1"/bad/EBS_GL_BALANCES_LINE.bad log="$path/$1"/log/EBS_GL_BALANCES_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_GL_BALANCES_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_gl_balances.csv',
'src_layer.EBS_GL_BALANCES_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_GL_BALANCES_LINE.log',
'$path/$1/bad/EBS_GL_BALANCES_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
 

  echo " ">>"$path"/error.log
 # ONLOAD TABLE EBS_FND_ID_FLEX_SEGMENTS_LINE
  echo "$T begin to load table EBS_FND_ID_FLEX_SEGMENTS_LINE"
  echo "[INFO] $T begin to load table EBS_FND_ID_FLEX_SEGMENTS_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_FND_ID_FLEX_SEGMENTS_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_fnd_id_flex_segments_$period.csv\""" "$ctl_path"/control/EBS_FND_ID_FLEX_SEGMENTS_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_FND_ID_FLEX_SEGMENTS_LINE.ctl bad="$path/$1"/bad/EBS_FND_ID_FLEX_SEGMENTS_LINE.bad log="$path/$1"/log/EBS_FND_ID_FLEX_SEGMENTS_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_FND_ID_FLEX_SEGMENTS_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_fnd_id_flex_segments.csv',
'src_layer.EBS_FND_ID_FLEX_SEGMENTS_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_FND_ID_FLEX_SEGMENTS_LINE.log',
'$path/$1/bad/EBS_FND_ID_FLEX_SEGMENTS_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_FND_FLEX_VALUE_SETS_LINE
  echo "$T begin to load table EBS_FND_FLEX_VALUE_SETS_LINE"
  echo "[INFO] $T begin to load table EBS_FND_FLEX_VALUE_SETS_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_FND_FLEX_VALUE_SETS_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_fnd_flex_value_sets_$period.csv\""" "$ctl_path"/control/EBS_FND_FLEX_VALUE_SETS_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_FND_FLEX_VALUE_SETS_LINE.ctl bad="$path/$1"/bad/EBS_FND_FLEX_VALUE_SETS_LINE.bad log="$path/$1"/log/EBS_FND_FLEX_VALUE_SETS_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_FND_FLEX_VALUE_SETS_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_fnd_flex_value_sets.csv',
'src_layer.EBS_FND_FLEX_VALUE_SETS_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_FND_FLEX_VALUE_SETS_LINE.log',
'$path/$1/bad/EBS_FND_FLEX_VALUE_SETS_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_FND_FLEX_VALUES_LINE
  echo "$T begin to load table EBS_FND_FLEX_VALUES_LINE"
  echo "[INFO] $T begin to load table EBS_FND_FLEX_VALUES_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_FND_FLEX_VALUES_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_fnd_flex_values_$period.csv\""" "$ctl_path"/control/EBS_FND_FLEX_VALUES_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_FND_FLEX_VALUES_LINE.ctl bad="$path/$1"/bad/EBS_FND_FLEX_VALUES_LINE.bad log="$path/$1"/log/EBS_FND_FLEX_VALUES_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_FND_FLEX_VALUES_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_fnd_flex_values.csv',
'src_layer.EBS_FND_FLEX_VALUES_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_FND_FLEX_VALUES_LINE.log',
'$path/$1/bad/EBS_FND_FLEX_VALUES_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_AP_SUPPLIERS_LINE
  echo "$T begin to load table EBS_AP_SUPPLIERS_LINE"
  echo "[INFO] $T begin to load table EBS_AP_SUPPLIERS_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_AP_SUPPLIERS_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_ap_suppliers_$period.csv\""" "$ctl_path"/control/EBS_AP_SUPPLIERS_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_AP_SUPPLIERS_LINE.ctl bad="$path/$1"/bad/EBS_AP_SUPPLIERS_LINE.bad log="$path/$1"/log/EBS_AP_SUPPLIERS_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_AP_SUPPLIERS_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_ap_suppliers.csv',
'src_layer.EBS_AP_SUPPLIERS_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_AP_SUPPLIERS_LINE.log',
'$path/$1/bad/EBS_AP_SUPPLIERS_LINE.bad');
commit;
exit;
EOF

  echo "*************end***************">>"$path"/error.log
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_AP_INVOICE_LINES_ALL_LINE
  echo "$T begin to load table EBS_AP_INVOICE_LINES_ALL_LINE"
  echo "[INFO] $T begin to load table EBS_AP_INVOICE_LINES_ALL_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_AP_INVOICE_LINES_ALL_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_ap_invoice_lines_all_$period.csv\""" "$ctl_path"/control/EBS_AP_INVOICE_LINES_ALL_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_AP_INVOICE_LINES_ALL_LINE.ctl bad="$path/$1"/bad/EBS_AP_INVOICE_LINES_ALL_LINE.bad log="$path/$1"/log/EBS_AP_INVOICE_LINES_ALL_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_AP_INVOICE_LINES_ALL_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_ap_invoice_lines_all.csv',
'src_layer.EBS_AP_INVOICE_LINES_ALL_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_AP_INVOICE_LINES_ALL_LINE.log',
'$path/$1/bad/EBS_AP_INVOICE_LINES_ALL_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  


echo "*************end***************">>"$path"/error.log
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE
  echo "$T begin to load table EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE"
  echo "[INFO] $T begin to load table EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_ap_invoice_distributions_all_$period.csv\""" "$ctl_path"/control/EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE.ctl bad="$path/$1"/bad/EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE.bad log="$path/$1"/log/EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_ap_invoice_distributions_all.csv',
'src_layer.EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE.log',
'$path/$1/bad/EBS_AP_INVOICE_DISTRIBUTIONS_ALL_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  

echo "*************end***************">>"$path"/error.log
  
  
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_AP_INVOICES_ALL_LINE
  echo "$T begin to load table EBS_AP_INVOICES_ALL_LINE"
  echo "[INFO] $T begin to load table EBS_AP_INVOICES_ALL_LINE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_AP_INVOICES_ALL_LINE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_ap_invoices_all_$period.csv\""" "$ctl_path"/control/EBS_AP_INVOICES_ALL_LINE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_AP_INVOICES_ALL_LINE.ctl bad="$path/$1"/bad/EBS_AP_INVOICES_ALL_LINE.bad log="$path/$1"/log/EBS_AP_INVOICES_ALL_LINE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_AP_INVOICES_ALL_LINE.bad ];then  
  is_success="no"
  else
  is_success="yes"
  fi
  # INSERT THE ETL RESULT INTO DATABASE  
  sqlplus etl_control/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
insert into ETL_CONTROL_LOG 
(ETL_MODLE,ETL_SOURCE,ETL_TARGET,DATA_DATE,ETL_DATE,IS_SUCCESS,LOG_PATH,ERROR_PATH) 
values 
('src_layer',
'$path/$1/csv/$1_ap_invoices_all.csv',
'src_layer.EBS_AP_INVOICES_ALL_LINE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_AP_INVOICES_ALL_LINE.log',
'$path/$1/bad/EBS_AP_INVOICES_ALL_LINE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
 
  
  
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi
