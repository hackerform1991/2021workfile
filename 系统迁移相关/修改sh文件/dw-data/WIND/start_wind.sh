#! /bin/bash
cd /oracle/dw-data/WIND
echo "strat to load data for csv document"
echo "date is " $1
DOCUMENTNAME="$1-READY"
echo "$DOCUMENTNAME will be used for etl"
T=`date +%D:%r`:
echo "NOW: $T"
# path location 
path="/oracle/dw-data/WIND"
# CONTROL FILE PATH
ctl_path="/oracle/dw-data/WIND"
echo " ">>"$path"/error.log
echo " ">>"$path"/error.log
echo "***********************">>"$path"/error.log
echo "start to etl the base data">>"$path"/error.log
echo "time: $T">>"$path"/error.log
echo "document: $1-WIND">>"$path"/error.log
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
  if [ -f "$path/$1-WIND.tar" ];then
    echo "[INFO] $T tar -xvf start:">>"$path"/error.log
    tar -xvf "$path/$1-WIND.tar" -C "$path"/"$1"/csv >>"$path"/error.log
  else 
    echo "$T $1-WIND.tar is not existed "
    echo "[ERROR] $T $1-WIND.tar is not existed ">>"$path"/error.log
  fi
  #######ONLOADDATE#######
 


 # ONLOAD TABLE WIND_CBONDANALYSISCNBD1
  echo "$T begin to load table WIND_CBONDANALYSISCNBD1"
  echo "[INFO] $T begin to load table WIND_CBONDANALYSISCNBD1">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDANALYSISCNBD1.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondAnalysisCNBD1.csv\""" "$ctl_path"/control/WIND_CBONDANALYSISCNBD1.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDANALYSISCNBD1.ctl bad="$path/$1"/bad/WIND_CBONDANALYSISCNBD1.bad log="$path/$1"/log/WIND_CBONDANALYSISCNBD1.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDANALYSISCNBD1.bad ];then  
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
'$path/$1/csv/$1-CBondAnalysisCNBD1.csv',
'src_layer.WIND_CBONDANALYSISCNBD1',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDANALYSISCNBD1.log',
'$path/$1/bad/WIND_CBONDANALYSISCNBD1.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE WIND_CBONDANALYSISCNBD2
  echo "$T begin to load table WIND_CBONDANALYSISCNBD2"
  echo "[INFO] $T begin to load table WIND_CBONDANALYSISCNBD2">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDANALYSISCNBD2.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondAnalysisCNBD2.csv\""" "$ctl_path"/control/WIND_CBONDANALYSISCNBD2.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDANALYSISCNBD2.ctl bad="$path/$1"/bad/WIND_CBONDANALYSISCNBD2.bad log="$path/$1"/log/WIND_CBONDANALYSISCNBD2.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDANALYSISCNBD2.bad ];then  
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
'$path/$1/csv/$1-CBondAnalysisCNBD2.csv',
'src_layer.WIND_CBONDANALYSISCNBD2',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDANALYSISCNBD2.log',
'$path/$1/bad/WIND_CBONDANALYSISCNBD2.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE WIND_CBONDANALYSISCNBD3
  echo "$T begin to load table WIND_CBONDANALYSISCNBD3"
  echo "[INFO] $T begin to load table WIND_CBONDANALYSISCNBD3">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDANALYSISCNBD3.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondAnalysisCNBD3.csv\""" "$ctl_path"/control/WIND_CBONDANALYSISCNBD3.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDANALYSISCNBD3.ctl bad="$path/$1"/bad/WIND_CBONDANALYSISCNBD3.bad log="$path/$1"/log/WIND_CBONDANALYSISCNBD3.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDANALYSISCNBD3.bad ];then  
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
'$path/$1/csv/$1-CBondAnalysisCNBD3.csv',
'src_layer.WIND_CBONDANALYSISCNBD3',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDANALYSISCNBD3.log',
'$path/$1/bad/WIND_CBONDANALYSISCNBD3.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE WIND_CBONDANALYSISCNBD4
  echo "$T begin to load table WIND_CBONDANALYSISCNBD4"
  echo "[INFO] $T begin to load table WIND_CBONDANALYSISCNBD4">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDANALYSISCNBD4.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondAnalysisCNBD4.csv\""" "$ctl_path"/control/WIND_CBONDANALYSISCNBD4.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDANALYSISCNBD4.ctl bad="$path/$1"/bad/WIND_CBONDANALYSISCNBD4.bad log="$path/$1"/log/WIND_CBONDANALYSISCNBD4.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDANALYSISCNBD4.bad ];then  
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
'$path/$1/csv/$1-CBondAnalysisCNBD4.csv',
'src_layer.WIND_CBONDANALYSISCNBD4',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDANALYSISCNBD4.log',
'$path/$1/bad/WIND_CBONDANALYSISCNBD4.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log


   # ONLOAD TABLE WIND_CBONDANALYSISCNBD5
  echo "$T begin to load table WIND_CBONDANALYSISCNBD5"
  echo "[INFO] $T begin to load table WIND_CBONDANALYSISCNBD5">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDANALYSISCNBD5.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondAnalysisCNBD5.csv\""" "$ctl_path"/control/WIND_CBONDANALYSISCNBD5.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDANALYSISCNBD5.ctl bad="$path/$1"/bad/WIND_CBONDANALYSISCNBD5.bad log="$path/$1"/log/WIND_CBONDANALYSISCNBD5.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDANALYSISCNBD5.bad ];then  
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
'$path/$1/csv/$1-CBondAnalysisCNBD5.csv',
'src_layer.WIND_CBONDANALYSISCNBD5',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDANALYSISCNBD5.log',
'$path/$1/bad/WIND_CBONDANALYSISCNBD5.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
   # ONLOAD TABLE WIND_CBONDANALYSISCNBD6
  echo "$T begin to load table WIND_CBONDANALYSISCNBD6"
  echo "[INFO] $T begin to load table WIND_CBONDANALYSISCNBD6">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDANALYSISCNBD6.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondAnalysisCNBD6.csv\""" "$ctl_path"/control/WIND_CBONDANALYSISCNBD6.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDANALYSISCNBD6.ctl bad="$path/$1"/bad/WIND_CBONDANALYSISCNBD6.bad log="$path/$1"/log/WIND_CBONDANALYSISCNBD6.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDANALYSISCNBD6.bad ];then  
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
'$path/$1/csv/$1-CBondAnalysisCNBD6.csv',
'src_layer.WIND_CBONDANALYSISCNBD6',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDANALYSISCNBD6.log',
'$path/$1/bad/WIND_CBONDANALYSISCNBD6.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
  
 # ONLOAD TABLE WIND_CBONDANALYSISCNBD7
  echo "$T begin to load table WIND_CBONDANALYSISCNBD7"
  echo "[INFO] $T begin to load table WIND_CBONDANALYSISCNBD7">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDANALYSISCNBD7.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondAnalysisCNBD7.csv\""" "$ctl_path"/control/WIND_CBONDANALYSISCNBD7.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDANALYSISCNBD7.ctl bad="$path/$1"/bad/WIND_CBONDANALYSISCNBD7.bad log="$path/$1"/log/WIND_CBONDANALYSISCNBD7.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDANALYSISCNBD7.bad ];then  
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
'$path/$1/csv/$1-CBondAnalysisCNBD7.csv',
'src_layer.WIND_CBONDANALYSISCNBD7',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDANALYSISCNBD7.log',
'$path/$1/bad/WIND_CBONDANALYSISCNBD7.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE WIND_CBONDANALYSISCSI
  echo "$T begin to load table WIND_CBONDANALYSISCSI"
  echo "[INFO] $T begin to load table WIND_CBONDANALYSISCSI">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDANALYSISCSI.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondAnalysisCSI.csv\""" "$ctl_path"/control/WIND_CBONDANALYSISCSI.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDANALYSISCSI.ctl bad="$path/$1"/bad/WIND_CBONDANALYSISCSI.bad log="$path/$1"/log/WIND_CBONDANALYSISCSI.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDANALYSISCSI.bad ];then  
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
'$path/$1/csv/$1-CBondAnalysisCSI.csv',
'src_layer.WIND_CBONDANALYSISCSI',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDANALYSISCSI.log',
'$path/$1/bad/WIND_CBONDANALYSISCSI.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE WIND_CBONDCOUNTERPRICE
  echo "$T begin to load table WIND_CBONDCOUNTERPRICE"
  echo "[INFO] $T begin to load table WIND_CBONDCOUNTERPRICE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDCOUNTERPRICE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondCounterPrice.csv\""" "$ctl_path"/control/WIND_CBONDCOUNTERPRICE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDCOUNTERPRICE.ctl bad="$path/$1"/bad/WIND_CBONDCOUNTERPRICE.bad log="$path/$1"/log/WIND_CBONDCOUNTERPRICE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDCOUNTERPRICE.bad ];then  
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
'$path/$1/csv/$1-CBondCounterPrice.csv',
'src_layer.WIND_CBONDCOUNTERPRICE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDCOUNTERPRICE.log',
'$path/$1/bad/WIND_CBONDCOUNTERPRICE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE WIND_CBONDCOUPONSSETTLEMENTFULL
  echo "$T begin to load table WIND_CBONDCOUPONSSETTLEMENTFULL"
  echo "[INFO] $T begin to load table WIND_CBONDCOUPONSSETTLEMENTFULL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDCOUPONSSETTLEMENTFULL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondCouponsSettlementFULL.csv\""" "$ctl_path"/control/WIND_CBONDCOUPONSSETTLEMENTFULL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDCOUPONSSETTLEMENTFULL.ctl bad="$path/$1"/bad/WIND_CBONDCOUPONSSETTLEMENTFULL.bad log="$path/$1"/log/WIND_CBONDCOUPONSSETTLEMENTFULL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDCOUPONSSETTLEMENTFULL.bad ];then  
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
'$path/$1/csv/$1-CBondCouponsSettlementFULL.csv',
'src_layer.WIND_CBONDCOUPONSSETTLEMENTFULL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDCOUPONSSETTLEMENTFULL.log',
'$path/$1/bad/WIND_CBONDCOUPONSSETTLEMENTFULL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  # ONLOAD TABLE WIND_CBONDCOUPONSSETTLEMENTNET
  echo "$T begin to load table WIND_CBONDCOUPONSSETTLEMENTNET"
  echo "[INFO] $T begin to load table WIND_CBONDCOUPONSSETTLEMENTNET">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDCOUPONSSETTLEMENTNET.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondCouponsSettlementNET.csv\""" "$ctl_path"/control/WIND_CBONDCOUPONSSETTLEMENTNET.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDCOUPONSSETTLEMENTNET.ctl bad="$path/$1"/bad/WIND_CBONDCOUPONSSETTLEMENTNET.bad log="$path/$1"/log/WIND_CBONDCOUPONSSETTLEMENTNET.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDCOUPONSSETTLEMENTNET.bad ];then  
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
'$path/$1/csv/$1-CBondCouponsSettlementNET.csv',
'src_layer.WIND_CBONDCOUPONSSETTLEMENTNET',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDCOUPONSSETTLEMENTNET.log',
'$path/$1/bad/WIND_CBONDCOUPONSSETTLEMENTNET.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE WIND_CBONDCURVECNBD
  echo "$T begin to load table WIND_CBONDCURVECNBD"
  echo "[INFO] $T begin to load table WIND_CBONDCURVECNBD">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDCURVECNBD.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondCurveCNBD.csv\""" "$ctl_path"/control/WIND_CBONDCURVECNBD.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDCURVECNBD.ctl bad="$path/$1"/bad/WIND_CBONDCURVECNBD.bad log="$path/$1"/log/WIND_CBONDCURVECNBD.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDCURVECNBD.bad ];then  
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
'$path/$1/csv/$1-CBondCurveCNBD.csv',
'src_layer.WIND_CBONDCURVECNBD',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDCURVECNBD.log',
'$path/$1/bad/WIND_CBONDCURVECNBD.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
   # ONLOAD TABLE WIND_CBONDCURVEMEMBERSCNBD
  echo "$T begin to load table WIND_CBONDCURVEMEMBERSCNBD"
  echo "[INFO] $T begin to load table WIND_CBONDCURVEMEMBERSCNBD">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDCURVEMEMBERSCNBD.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondCurveMembersCNBD.csv\""" "$ctl_path"/control/WIND_CBONDCURVEMEMBERSCNBD.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDCURVEMEMBERSCNBD.ctl bad="$path/$1"/bad/WIND_CBONDCURVEMEMBERSCNBD.bad log="$path/$1"/log/WIND_CBONDCURVEMEMBERSCNBD.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDCURVEMEMBERSCNBD.bad ];then  
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
'$path/$1/csv/$1-CBondCurveMembersCNBD.csv',
'src_layer.WIND_CBONDCURVEMEMBERSCNBD',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDCURVEMEMBERSCNBD.log',
'$path/$1/bad/WIND_CBONDCURVEMEMBERSCNBD.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE WIND_CBONDDESCRIPTION
  echo "$T begin to load table WIND_CBONDDESCRIPTION"
  echo "[INFO] $T begin to load table WIND_CBONDDESCRIPTION">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDDESCRIPTION.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondDescription.csv\""" "$ctl_path"/control/WIND_CBONDDESCRIPTION.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDDESCRIPTION.ctl bad="$path/$1"/bad/WIND_CBONDDESCRIPTION.bad log="$path/$1"/log/WIND_CBONDDESCRIPTION.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDDESCRIPTION.bad ];then  
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
'$path/$1/csv/$1-CBondDescription.csv',
'src_layer.WIND_CBONDDESCRIPTION',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDDESCRIPTION.log',
'$path/$1/bad/WIND_CBONDDESCRIPTION.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE WIND_CBONDFLOWCOEFFICIENTCNBD
  echo "$T begin to load table WIND_CBONDFLOWCOEFFICIENTCNBD"
  echo "[INFO] $T begin to load table WIND_CBONDFLOWCOEFFICIENTCNBD">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDFLOWCOEFFICIENTCNBD.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondFlowCoefficientCNBD.csv\""" "$ctl_path"/control/WIND_CBONDFLOWCOEFFICIENTCNBD.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDFLOWCOEFFICIENTCNBD.ctl bad="$path/$1"/bad/WIND_CBONDFLOWCOEFFICIENTCNBD.bad log="$path/$1"/log/WIND_CBONDFLOWCOEFFICIENTCNBD.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDFLOWCOEFFICIENTCNBD.bad ];then  
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
'$path/$1/csv/$1-CBondFlowCoefficientCNBD.csv',
'src_layer.WIND_CBONDFLOWCOEFFICIENTCNBD',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDFLOWCOEFFICIENTCNBD.log',
'$path/$1/bad/WIND_CBONDFLOWCOEFFICIENTCNBD.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE WIND_CBONDISSUERRATING
  echo "$T begin to load table WIND_CBONDISSUERRATING"
  echo "[INFO] $T begin to load table WIND_CBONDISSUERRATING">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDISSUERRATING.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondIssuerRating.csv\""" "$ctl_path"/control/WIND_CBONDISSUERRATING.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDISSUERRATING.ctl bad="$path/$1"/bad/WIND_CBONDISSUERRATING.bad log="$path/$1"/log/WIND_CBONDISSUERRATING.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDISSUERRATING.bad ];then  
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
'$path/$1/csv/$1-CBondIssuerRating.csv',
'src_layer.WIND_CBONDISSUERRATING',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDISSUERRATING.log',
'$path/$1/bad/WIND_CBONDISSUERRATING.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

 # ONLOAD TABLE WIND_CBONDRATING
  echo "$T begin to load table WIND_CBONDRATING"
  echo "[INFO] $T begin to load table WIND_CBONDRATING">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CBONDRATING.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CBondRating.csv\""" "$ctl_path"/control/WIND_CBONDRATING.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CBONDRATING.ctl bad="$path/$1"/bad/WIND_CBONDRATING.bad log="$path/$1"/log/WIND_CBONDRATING.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CBONDRATING.bad ];then  
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
'$path/$1/csv/$1-CBondRating.csv',
'src_layer.WIND_CBONDRATING',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CBONDRATING.log',
'$path/$1/bad/WIND_CBONDRATING.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

   # ONLOAD TABLE WIND_CCBONDCONVERSION
  echo "$T begin to load table WIND_CCBONDCONVERSION"
  echo "[INFO] $T begin to load table WIND_CCBONDCONVERSION">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CCBONDCONVERSION.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CCBondConversion.csv\""" "$ctl_path"/control/WIND_CCBONDCONVERSION.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CCBONDCONVERSION.ctl bad="$path/$1"/bad/WIND_CCBONDCONVERSION.bad log="$path/$1"/log/WIND_CCBONDCONVERSION.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CCBONDCONVERSION.bad ];then  
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
'$path/$1/csv/$1-CCBondConversion.csv',
'src_layer.WIND_CCBONDCONVERSION',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CCBONDCONVERSION.log',
'$path/$1/bad/WIND_CCBONDCONVERSION.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE WIND_CGBBENCHMARK
  echo "$T begin to load table WIND_CGBBENCHMARK"
  echo "[INFO] $T begin to load table WIND_CGBBENCHMARK">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_CGBBENCHMARK.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CGBbenchmark.csv\""" "$ctl_path"/control/WIND_CGBBENCHMARK.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_CGBBENCHMARK.ctl bad="$path/$1"/bad/WIND_CGBBENCHMARK.bad log="$path/$1"/log/WIND_CGBBENCHMARK.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_CGBBENCHMARK.bad ];then  
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
'$path/$1/csv/$1-CGBbenchmark.csv',
'src_layer.WIND_CGBBENCHMARK',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_CGBBENCHMARK.log',
'$path/$1/bad/WIND_CGBBENCHMARK.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE WIND_SHIBORPRICES
  echo "$T begin to load table WIND_SHIBORPRICES"
  echo "[INFO] $T begin to load table WIND_SHIBORPRICES">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/WIND_SHIBORPRICES.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-shiborPrices.csv\""" "$ctl_path"/control/WIND_SHIBORPRICES.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/WIND_SHIBORPRICES.ctl bad="$path/$1"/bad/WIND_SHIBORPRICES.bad log="$path/$1"/log/WIND_SHIBORPRICES.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/WIND_SHIBORPRICES.bad ];then  
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
'$path/$1/csv/$1-shiborPrices.csv',
'src_layer.WIND_SHIBORPRICES',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/WIND_SHIBORPRICES.log',
'$path/$1/bad/WIND_SHIBORPRICES.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
else
   echo "$T the ready document is not exist ,please check whether the file is ready "
   echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi


  
  
  
  