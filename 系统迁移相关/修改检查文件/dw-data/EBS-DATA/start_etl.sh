#! /bin/bash

. ~/.profile 
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
  #get filename
  temp=${file##*/}
  filename=${temp%.*}
  echo filename is $filename  
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
  sqlldr src_layer/oracle@ICBCAMCDW control="$path"/control/EBS_DATA_HEADER.ctl bad="$path/$1"/bad/"$filename"_HEADER.bad log="$path/$1"/log/"$filename"_HEADER.log>>"$path"/error.log
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
  
  
  echo $period
  echo " ">>"$path"/error.log
 # ONLOAD TABLE EBS_GL_LEDGERS
  echo "$T begin to load table EBS_GL_LEDGERS"
  echo "[INFO] $T begin to load table EBS_GL_LEDGERS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_GL_LEDGERS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_gl_ledgers_$period.csv\""" "$ctl_path"/control/EBS_GL_LEDGERS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_GL_LEDGERS.ctl bad="$path/$1"/bad/EBS_GL_LEDGERS.bad log="$path/$1"/log/EBS_GL_LEDGERS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_GL_LEDGERS.bad ];then  
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
'src_layer.EBS_GL_LEDGERS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_GL_LEDGERS.log',
'$path/$1/bad/EBS_GL_LEDGERS.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  

  echo " ">>"$path"/error.log
 # ONLOAD TABLE EBS_GL_CODE_COMBINATIONS
  echo "$T begin to load table EBS_GL_CODE_COMBINATIONS"
  echo "[INFO] $T begin to load table EBS_GL_CODE_COMBINATIONS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_GL_CODE_COMBINATIONS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_gl_code_combinations_$period.csv\""" "$ctl_path"/control/EBS_GL_CODE_COMBINATIONS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_GL_CODE_COMBINATIONS.ctl bad="$path/$1"/bad/EBS_GL_CODE_COMBINATIONS.bad log="$path/$1"/log/EBS_GL_CODE_COMBINATIONS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_GL_CODE_COMBINATIONS.bad ];then  
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
'src_layer.EBS_GL_CODE_COMBINATIONS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_GL_CODE_COMBINATIONS.log',
'$path/$1/bad/EBS_GL_CODE_COMBINATIONS.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  echo " ">>"$path"/error.log
 # ONLOAD TABLE EBS_GL_BALANCES
  echo "$T begin to load table EBS_GL_BALANCES"
  echo "[INFO] $T begin to load table EBS_GL_BALANCES">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_GL_BALANCES.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_gl_balances_$period.csv\""" "$ctl_path"/control/EBS_GL_BALANCES.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_GL_BALANCES.ctl bad="$path/$1"/bad/EBS_GL_BALANCES.bad log="$path/$1"/log/EBS_GL_BALANCES.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_GL_BALANCES.bad ];then  
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
'src_layer.EBS_GL_BALANCES',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_GL_BALANCES.log',
'$path/$1/bad/EBS_GL_BALANCES.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
 

  echo " ">>"$path"/error.log
 # ONLOAD TABLE EBS_FND_ID_FLEX_SEGMENTS
  echo "$T begin to load table EBS_FND_ID_FLEX_SEGMENTS"
  echo "[INFO] $T begin to load table EBS_FND_ID_FLEX_SEGMENTS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_FND_ID_FLEX_SEGMENTS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_fnd_id_flex_segments_$period.csv\""" "$ctl_path"/control/EBS_FND_ID_FLEX_SEGMENTS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_FND_ID_FLEX_SEGMENTS.ctl bad="$path/$1"/bad/EBS_FND_ID_FLEX_SEGMENTS.bad log="$path/$1"/log/EBS_FND_ID_FLEX_SEGMENTS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_FND_ID_FLEX_SEGMENTS.bad ];then  
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
'src_layer.EBS_FND_ID_FLEX_SEGMENTS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_FND_ID_FLEX_SEGMENTS.log',
'$path/$1/bad/EBS_FND_ID_FLEX_SEGMENTS.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_FND_FLEX_VALUE_SETS
  echo "$T begin to load table EBS_FND_FLEX_VALUE_SETS"
  echo "[INFO] $T begin to load table EBS_FND_FLEX_VALUE_SETS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_FND_FLEX_VALUE_SETS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_fnd_flex_value_sets_$period.csv\""" "$ctl_path"/control/EBS_FND_FLEX_VALUE_SETS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_FND_FLEX_VALUE_SETS.ctl bad="$path/$1"/bad/EBS_FND_FLEX_VALUE_SETS.bad log="$path/$1"/log/EBS_FND_FLEX_VALUE_SETS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_FND_FLEX_VALUE_SETS.bad ];then  
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
'src_layer.EBS_FND_FLEX_VALUE_SETS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_FND_FLEX_VALUE_SETS.log',
'$path/$1/bad/EBS_FND_FLEX_VALUE_SETS.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_FND_FLEX_VALUES
  echo "$T begin to load table EBS_FND_FLEX_VALUES"
  echo "[INFO] $T begin to load table EBS_FND_FLEX_VALUES">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_FND_FLEX_VALUES.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_fnd_flex_values_$period.csv\""" "$ctl_path"/control/EBS_FND_FLEX_VALUES.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_FND_FLEX_VALUES.ctl bad="$path/$1"/bad/EBS_FND_FLEX_VALUES.bad log="$path/$1"/log/EBS_FND_FLEX_VALUES.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_FND_FLEX_VALUES.bad ];then  
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
'src_layer.EBS_FND_FLEX_VALUES',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_FND_FLEX_VALUES.log',
'$path/$1/bad/EBS_FND_FLEX_VALUES.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_AP_SUPPLIERS
  echo "$T begin to load table EBS_AP_SUPPLIERS"
  echo "[INFO] $T begin to load table EBS_AP_SUPPLIERS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_AP_SUPPLIERS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_ap_suppliers_$period.csv\""" "$ctl_path"/control/EBS_AP_SUPPLIERS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_AP_SUPPLIERS.ctl bad="$path/$1"/bad/EBS_AP_SUPPLIERS.bad log="$path/$1"/log/EBS_AP_SUPPLIERS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_AP_SUPPLIERS.bad ];then  
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
'src_layer.EBS_AP_SUPPLIERS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_AP_SUPPLIERS.log',
'$path/$1/bad/EBS_AP_SUPPLIERS.bad');
commit;
exit;
EOF

  echo "*************end***************">>"$path"/error.log
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_AP_INVOICE_LINES_ALL
  echo "$T begin to load table EBS_AP_INVOICE_LINES_ALL"
  echo "[INFO] $T begin to load table EBS_AP_INVOICE_LINES_ALL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_AP_INVOICE_LINES_ALL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_ap_invoice_lines_all_$period.csv\""" "$ctl_path"/control/EBS_AP_INVOICE_LINES_ALL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_AP_INVOICE_LINES_ALL.ctl bad="$path/$1"/bad/EBS_AP_INVOICE_LINES_ALL.bad log="$path/$1"/log/EBS_AP_INVOICE_LINES_ALL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_AP_INVOICE_LINES_ALL.bad ];then  
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
'src_layer.EBS_AP_INVOICE_LINES_ALL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_AP_INVOICE_LINES_ALL.log',
'$path/$1/bad/EBS_AP_INVOICE_LINES_ALL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  


echo "*************end***************">>"$path"/error.log
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_AP_INVOICE_DISTRIBUTIONS_ALL
  echo "$T begin to load table EBS_AP_INVOICE_DISTRIBUTIONS_ALL"
  echo "[INFO] $T begin to load table EBS_AP_INVOICE_DISTRIBUTIONS_ALL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_AP_INVOICE_DISTRIBUTIONS_ALL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_ap_invoice_distributions_all_$period.csv\""" "$ctl_path"/control/EBS_AP_INVOICE_DISTRIBUTIONS_ALL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_AP_INVOICE_DISTRIBUTIONS_ALL.ctl bad="$path/$1"/bad/EBS_AP_INVOICE_DISTRIBUTIONS_ALL.bad log="$path/$1"/log/EBS_AP_INVOICE_DISTRIBUTIONS_ALL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_AP_INVOICE_DISTRIBUTIONS_ALL.bad ];then  
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
'src_layer.EBS_AP_INVOICE_DISTRIBUTIONS_ALL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_AP_INVOICE_DISTRIBUTIONS_ALL.log',
'$path/$1/bad/EBS_AP_INVOICE_DISTRIBUTIONS_ALL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  

echo "*************end***************">>"$path"/error.log
  
  
  echo " ">>"$path"/error.log
  #ONLOAD TABLE EBS_AP_INVOICES_ALL
  echo "$T begin to load table EBS_AP_INVOICES_ALL"
  echo "[INFO] $T begin to load table EBS_AP_INVOICES_ALL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/EBS_AP_INVOICES_ALL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1_ap_invoices_all_$period.csv\""" "$ctl_path"/control/EBS_AP_INVOICES_ALL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/EBS_AP_INVOICES_ALL.ctl bad="$path/$1"/bad/EBS_AP_INVOICES_ALL.bad log="$path/$1"/log/EBS_AP_INVOICES_ALL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/EBS_AP_INVOICES_ALL.bad ];then  
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
'src_layer.EBS_AP_INVOICES_ALL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/EBS_AP_INVOICES_ALL.log',
'$path/$1/bad/EBS_AP_INVOICES_ALL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
 
  
  
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi
