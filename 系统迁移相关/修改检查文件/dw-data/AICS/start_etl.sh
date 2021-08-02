#! /bin/bash
cd /dw-data/AICS
echo "strat to load data for csv document"
echo "date is " $1
DOCUMENTNAME="$1-READY"
echo "$DOCUMENTNAME will be used for etl"
T=`date +%D:%r`:
echo "NOW: $T"
# path location 
path="/dw-data/AICS"
# CONTROL FILE PATH
ctl_path="/dw-data/AICS"
echo " ">>"$path"/error.log
echo " ">>"$path"/error.log
echo "***********************">>"$path"/error.log
echo "start to etl the base data">>"$path"/error.log
echo "time: $T">>"$path"/error.log
echo "document: $1-AICS">>"$path"/error.log
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
  if [ -f "$path/$1-AICS.tar" ];then
    echo "[INFO] $T tar -xvf start:">>"$path"/error.log
    tar -xvf "$path/$1-AICS.tar" -C "$path"/"$1"/csv >>"$path"/error.log
  else 
    echo "$T $1-AICS.tar is not existed "
    echo "[ERROR] $T $1-AICS.tar is not existed ">>"$path"/error.log
  fi
  #######ONLOADDATE#######
 


 # ONLOAD TABLE AICS_ACCOUNT_BANKINFO
  echo "$T begin to load table AICS_ACCOUNT_BANKINFO"
  echo "[INFO] $T begin to load table AICS_ACCOUNT_BANKINFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_ACCOUNT_BANKINFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-AccountBankinfo.csv\""" "$ctl_path"/control/AICS_ACCOUNT_BANKINFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_ACCOUNT_BANKINFO.ctl bad="$path/$1"/bad/AICS_ACCOUNT_BANKINFO.bad log="$path/$1"/log/AICS_ACCOUNT_BANKINFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_ACCOUNT_BANKINFO.bad ];then  
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
'$path/$1/csv/$1-AccountBankinfo.csv',
'src_layer.AICS_ACCOUNT_BANKINFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_ACCOUNT_BANKINFO.log',
'$path/$1/bad/AICS_ACCOUNT_BANKINFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE AICS_ASSET_CREDITOR_RELATION
  echo "$T begin to load table AICS_ASSET_CREDITOR_RELATION"
  echo "[INFO] $T begin to load table AICS_ASSET_CREDITOR_RELATION">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_ASSET_CREDITOR_RELATION.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-AssetCreditorRelation.csv\""" "$ctl_path"/control/AICS_ASSET_CREDITOR_RELATION.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_ASSET_CREDITOR_RELATION.ctl bad="$path/$1"/bad/AICS_ASSET_CREDITOR_RELATION.bad log="$path/$1"/log/AICS_ASSET_CREDITOR_RELATION.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_ASSET_CREDITOR_RELATION.bad ];then  
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
'$path/$1/csv/$1-AssetCreditorRelation.csv',
'src_layer.AICS_ASSET_CREDITOR_RELATION',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_ASSET_CREDITOR_RELATION.log',
'$path/$1/bad/AICS_ASSET_CREDITOR_RELATION.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE AICS_ASSET_CREDITORCONTRACT
  echo "$T begin to load table AICS_ASSET_CREDITORCONTRACT"
  echo "[INFO] $T begin to load table AICS_ASSET_CREDITORCONTRACT">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_ASSET_CREDITORCONTRACT.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-AssetCreditorContract.csv\""" "$ctl_path"/control/AICS_ASSET_CREDITORCONTRACT.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_ASSET_CREDITORCONTRACT.ctl bad="$path/$1"/bad/AICS_ASSET_CREDITORCONTRACT.bad log="$path/$1"/log/AICS_ASSET_CREDITORCONTRACT.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_ASSET_CREDITORCONTRACT.bad ];then  
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
'$path/$1/csv/$1-AssetCreditorContract.csv',
'src_layer.AICS_ASSET_CREDITORCONTRACT',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_ASSET_CREDITORCONTRACT.log',
'$path/$1/bad/AICS_ASSET_CREDITORCONTRACT.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE AICS_ASSET_GUARANTEECONTRACT
  echo "$T begin to load table AICS_ASSET_GUARANTEECONTRACT"
  echo "[INFO] $T begin to load table AICS_ASSET_GUARANTEECONTRACT">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_ASSET_GUARANTEECONTRACT.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-AssetGuaranteeContract.csv\""" "$ctl_path"/control/AICS_ASSET_GUARANTEECONTRACT.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_ASSET_GUARANTEECONTRACT.ctl bad="$path/$1"/bad/AICS_ASSET_GUARANTEECONTRACT.bad log="$path/$1"/log/AICS_ASSET_GUARANTEECONTRACT.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_ASSET_GUARANTEECONTRACT.bad ];then  
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
'$path/$1/csv/$1-AssetGuaranteeContract.csv',
'src_layer.AICS_ASSET_GUARANTEECONTRACT',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_ASSET_GUARANTEECONTRACT.log',
'$path/$1/bad/AICS_ASSET_GUARANTEECONTRACT.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log


   # ONLOAD TABLE AICS_ASSET_LOANACCOUNT
  echo "$T begin to load table AICS_ASSET_LOANACCOUNT"
  echo "[INFO] $T begin to load table AICS_ASSET_LOANACCOUNT">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_ASSET_LOANACCOUNT.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-AssetLoanaccount.csv\""" "$ctl_path"/control/AICS_ASSET_LOANACCOUNT.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_ASSET_LOANACCOUNT.ctl bad="$path/$1"/bad/AICS_ASSET_LOANACCOUNT.bad log="$path/$1"/log/AICS_ASSET_LOANACCOUNT.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_ASSET_LOANACCOUNT.bad ];then  
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
'$path/$1/csv/$1-AssetLoanaccount.csv',
'src_layer.AICS_ASSET_LOANACCOUNT',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_ASSET_LOANACCOUNT.log',
'$path/$1/bad/AICS_ASSET_LOANACCOUNT.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
   # ONLOAD TABLE AICS_ASSET_LOANACCOUNT_INFO
  echo "$T begin to load table AICS_ASSET_LOANACCOUNT_INFO"
  echo "[INFO] $T begin to load table AICS_ASSET_LOANACCOUNT_INFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_ASSET_LOANACCOUNT_INFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-AssetLoanAccountInfo.csv\""" "$ctl_path"/control/AICS_ASSET_LOANACCOUNT_INFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_ASSET_LOANACCOUNT_INFO.ctl bad="$path/$1"/bad/AICS_ASSET_LOANACCOUNT_INFO.bad log="$path/$1"/log/AICS_ASSET_LOANACCOUNT_INFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_ASSET_LOANACCOUNT_INFO.bad ];then  
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
'$path/$1/csv/$1-AssetLoanAccountInfo.csv',
'src_layer.AICS_ASSET_LOANACCOUNT_INFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_ASSET_LOANACCOUNT_INFO.log',
'$path/$1/bad/AICS_ASSET_LOANACCOUNT_INFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
  
 # ONLOAD TABLE AICS_ASSET_MORTGAGECONTRACT
  echo "$T begin to load table AICS_ASSET_MORTGAGECONTRACT"
  echo "[INFO] $T begin to load table AICS_ASSET_MORTGAGECONTRACT">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_ASSET_MORTGAGECONTRACT.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-AssetMortgageContract.csv\""" "$ctl_path"/control/AICS_ASSET_MORTGAGECONTRACT.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_ASSET_MORTGAGECONTRACT.ctl bad="$path/$1"/bad/AICS_ASSET_MORTGAGECONTRACT.bad log="$path/$1"/log/AICS_ASSET_MORTGAGECONTRACT.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_ASSET_MORTGAGECONTRACT.bad ];then  
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
'$path/$1/csv/$1-AssetMortgageContract.csv',
'src_layer.AICS_ASSET_MORTGAGECONTRACT',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_ASSET_MORTGAGECONTRACT.log',
'$path/$1/bad/AICS_ASSET_MORTGAGECONTRACT.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE AICS_ASSET_PAWN_INFO
  echo "$T begin to load table AICS_ASSET_PAWN_INFO"
  echo "[INFO] $T begin to load table AICS_ASSET_PAWN_INFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_ASSET_PAWN_INFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-AssetPawnInfo.csv\""" "$ctl_path"/control/AICS_ASSET_PAWN_INFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_ASSET_PAWN_INFO.ctl bad="$path/$1"/bad/AICS_ASSET_PAWN_INFO.bad log="$path/$1"/log/AICS_ASSET_PAWN_INFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_ASSET_PAWN_INFO.bad ];then  
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
'$path/$1/csv/$1-AssetPawnInfo.csv',
'src_layer.AICS_ASSET_PAWN_INFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_ASSET_PAWN_INFO.log',
'$path/$1/bad/AICS_ASSET_PAWN_INFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE AICS_CUSTOMERINFO
  echo "$T begin to load table AICS_CUSTOMERINFO"
  echo "[INFO] $T begin to load table AICS_CUSTOMERINFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_CUSTOMERINFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-Customerinfo.csv\""" "$ctl_path"/control/AICS_CUSTOMERINFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_CUSTOMERINFO.ctl bad="$path/$1"/bad/AICS_CUSTOMERINFO.bad log="$path/$1"/log/AICS_CUSTOMERINFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_CUSTOMERINFO.bad ];then  
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
'$path/$1/csv/$1-Customerinfo.csv',
'src_layer.AICS_CUSTOMERINFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_CUSTOMERINFO.log',
'$path/$1/bad/AICS_CUSTOMERINFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE AICS_DEPOSITS_LOANS_DEAL
  echo "$T begin to load table AICS_DEPOSITS_LOANS_DEAL"
  echo "[INFO] $T begin to load table AICS_DEPOSITS_LOANS_DEAL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_DEPOSITS_LOANS_DEAL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-DepositsLoansDeal.csv\""" "$ctl_path"/control/AICS_DEPOSITS_LOANS_DEAL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_DEPOSITS_LOANS_DEAL.ctl bad="$path/$1"/bad/AICS_DEPOSITS_LOANS_DEAL.bad log="$path/$1"/log/AICS_DEPOSITS_LOANS_DEAL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_DEPOSITS_LOANS_DEAL.bad ];then  
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
'$path/$1/csv/$1-DepositsLoansDeal.csv',
'src_layer.AICS_DEPOSITS_LOANS_DEAL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_DEPOSITS_LOANS_DEAL.log',
'$path/$1/bad/AICS_DEPOSITS_LOANS_DEAL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  # ONLOAD TABLE AICS_EQUITY_ASSETS_INFO
  echo "$T begin to load table AICS_EQUITY_ASSETS_INFO"
  echo "[INFO] $T begin to load table AICS_EQUITY_ASSETS_INFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_EQUITY_ASSETS_INFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-EquityAssetInfo.csv\""" "$ctl_path"/control/AICS_EQUITY_ASSETS_INFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_EQUITY_ASSETS_INFO.ctl bad="$path/$1"/bad/AICS_EQUITY_ASSETS_INFO.bad log="$path/$1"/log/AICS_EQUITY_ASSETS_INFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_EQUITY_ASSETS_INFO.bad ];then  
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
'$path/$1/csv/$1-EquityAssetInfo.csv',
'src_layer.AICS_EQUITY_ASSETS_INFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_EQUITY_ASSETS_INFO.log',
'$path/$1/bad/AICS_EQUITY_ASSETS_INFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE AICS_INVEST_CONTRACT
  echo "$T begin to load table AICS_INVEST_CONTRACT"
  echo "[INFO] $T begin to load table AICS_INVEST_CONTRACT">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_INVEST_CONTRACT.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-InvestContract.csv\""" "$ctl_path"/control/AICS_INVEST_CONTRACT.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_INVEST_CONTRACT.ctl bad="$path/$1"/bad/AICS_INVEST_CONTRACT.bad log="$path/$1"/log/AICS_INVEST_CONTRACT.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_INVEST_CONTRACT.bad ];then  
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
'$path/$1/csv/$1-InvestContract.csv',
'src_layer.AICS_INVEST_CONTRACT',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_INVEST_CONTRACT.log',
'$path/$1/bad/AICS_INVEST_CONTRACT.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
   # ONLOAD TABLE AICS_INVEST_FLOATRATE
  echo "$T begin to load table AICS_INVEST_FLOATRATE"
  echo "[INFO] $T begin to load table AICS_INVEST_FLOATRATE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_INVEST_FLOATRATE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-InvestFloatRate.csv\""" "$ctl_path"/control/AICS_INVEST_FLOATRATE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_INVEST_FLOATRATE.ctl bad="$path/$1"/bad/AICS_INVEST_FLOATRATE.bad log="$path/$1"/log/AICS_INVEST_FLOATRATE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_INVEST_FLOATRATE.bad ];then  
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
'$path/$1/csv/$1-InvestFloatRate.csv',
'src_layer.AICS_INVEST_FLOATRATE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_INVEST_FLOATRATE.log',
'$path/$1/bad/AICS_INVEST_FLOATRATE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE AICS_INVEST_PLAN
  echo "$T begin to load table AICS_INVEST_PLAN"
  echo "[INFO] $T begin to load table AICS_INVEST_PLAN">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_INVEST_PLAN.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-InvestPlan.csv\""" "$ctl_path"/control/AICS_INVEST_PLAN.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_INVEST_PLAN.ctl bad="$path/$1"/bad/AICS_INVEST_PLAN.bad log="$path/$1"/log/AICS_INVEST_PLAN.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_INVEST_PLAN.bad ];then  
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
'$path/$1/csv/$1-InvestPlan.csv',
'src_layer.AICS_INVEST_PLAN',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_INVEST_PLAN.log',
'$path/$1/bad/AICS_INVEST_PLAN.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE AICS_INVEST_PLANDETAIL
  echo "$T begin to load table AICS_INVEST_PLANDETAIL"
  echo "[INFO] $T begin to load table AICS_INVEST_PLANDETAIL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_INVEST_PLANDETAIL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-InvestPlanDetail.csv\""" "$ctl_path"/control/AICS_INVEST_PLANDETAIL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_INVEST_PLANDETAIL.ctl bad="$path/$1"/bad/AICS_INVEST_PLANDETAIL.bad log="$path/$1"/log/AICS_INVEST_PLANDETAIL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_INVEST_PLANDETAIL.bad ];then  
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
'$path/$1/csv/$1-InvestPlanDetail.csv',
'src_layer.AICS_INVEST_PLANDETAIL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_INVEST_PLANDETAIL.log',
'$path/$1/bad/AICS_INVEST_PLANDETAIL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE AICS_INVEST_RATE
  echo "$T begin to load table AICS_INVEST_RATE"
  echo "[INFO] $T begin to load table AICS_INVEST_RATE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_INVEST_RATE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-InvestRate.csv\""" "$ctl_path"/control/AICS_INVEST_RATE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_INVEST_RATE.ctl bad="$path/$1"/bad/AICS_INVEST_RATE.bad log="$path/$1"/log/AICS_INVEST_RATE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_INVEST_RATE.bad ];then  
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
'$path/$1/csv/$1-InvestRate.csv',
'src_layer.AICS_INVEST_RATE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_INVEST_RATE.log',
'$path/$1/bad/AICS_INVEST_RATE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

   # ONLOAD TABLE AICS_PAYMENT_RECEIPT
  echo "$T begin to load table AICS_PAYMENT_RECEIPT"
  echo "[INFO] $T begin to load table AICS_PAYMENT_RECEIPT">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_PAYMENT_RECEIPT.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-PaymentReceipt.csv\""" "$ctl_path"/control/AICS_PAYMENT_RECEIPT.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_PAYMENT_RECEIPT.ctl bad="$path/$1"/bad/AICS_PAYMENT_RECEIPT.bad log="$path/$1"/log/AICS_PAYMENT_RECEIPT.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_PAYMENT_RECEIPT.bad ];then  
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
'$path/$1/csv/$1-PaymentReceipt.csv',
'src_layer.AICS_PAYMENT_RECEIPT',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_PAYMENT_RECEIPT.log',
'$path/$1/bad/AICS_PAYMENT_RECEIPT.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
   # ONLOAD TABLE AICS_PROJECT_INFO
  echo "$T begin to load table AICS_PROJECT_INFO"
  echo "[INFO] $T begin to load table AICS_PROJECT_INFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_PROJECT_INFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-ProjectInfo.csv\""" "$ctl_path"/control/AICS_PROJECT_INFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_PROJECT_INFO.ctl bad="$path/$1"/bad/AICS_PROJECT_INFO.bad log="$path/$1"/log/AICS_PROJECT_INFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_PROJECT_INFO.bad ];then  
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
'$path/$1/csv/$1-ProjectInfo.csv',
'src_layer.AICS_PROJECT_INFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_PROJECT_INFO.log',
'$path/$1/bad/AICS_PROJECT_INFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  

   # ONLOAD TABLE AICS_REPO_DEAL
  echo "$T begin to load table AICS_REPO_DEAL"
  echo "[INFO] $T begin to load table AICS_REPO_DEAL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_REPO_DEAL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-RepoDeal.csv\""" "$ctl_path"/control/AICS_REPO_DEAL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_REPO_DEAL.ctl bad="$path/$1"/bad/AICS_REPO_DEAL.bad log="$path/$1"/log/AICS_REPO_DEAL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_REPO_DEAL.bad ];then  
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
'$path/$1/csv/$1-RepoDeal.csv',
'src_layer.AICS_REPO_DEAL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_REPO_DEAL.log',
'$path/$1/bad/AICS_REPO_DEAL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log

  
  
   # ONLOAD TABLE AICS_SECURITY_DEAL
  echo "$T begin to load table AICS_SECURITY_DEAL"
  echo "[INFO] $T begin to load table AICS_SECURITY_DEAL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_DEAL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityDeal.csv\""" "$ctl_path"/control/AICS_SECURITY_DEAL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_DEAL.ctl bad="$path/$1"/bad/AICS_SECURITY_DEAL.bad log="$path/$1"/log/AICS_SECURITY_DEAL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_DEAL.bad ];then  
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
'$path/$1/csv/$1-SecurityDeal.csv',
'src_layer.AICS_SECURITY_DEAL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_DEAL.log',
'$path/$1/bad/AICS_SECURITY_DEAL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE AICS_SECURITY_ISSUANCE
  echo "$T begin to load table AICS_SECURITY_ISSUANCE"
  echo "[INFO] $T begin to load table AICS_SECURITY_ISSUANCE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_ISSUANCE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityIssuance.csv\""" "$ctl_path"/control/AICS_SECURITY_ISSUANCE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_ISSUANCE.ctl bad="$path/$1"/bad/AICS_SECURITY_ISSUANCE.bad log="$path/$1"/log/AICS_SECURITY_ISSUANCE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_ISSUANCE.bad ];then  
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
'$path/$1/csv/$1-SecurityIssuance.csv',
'src_layer.AICS_SECURITY_ISSUANCE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_ISSUANCE.log',
'$path/$1/bad/AICS_SECURITY_ISSUANCE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE AICS_SECURITY_MASTER
  echo "$T begin to load table AICS_SECURITY_MASTER"
  echo "[INFO] $T begin to load table AICS_SECURITY_MASTER">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_MASTER.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityMaster.csv\""" "$ctl_path"/control/AICS_SECURITY_MASTER.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_MASTER.ctl bad="$path/$1"/bad/AICS_SECURITY_MASTER.bad log="$path/$1"/log/AICS_SECURITY_MASTER.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_MASTER.bad ];then  
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
'$path/$1/csv/$1-SecurityMaster.csv',
'src_layer.AICS_SECURITY_MASTER',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_MASTER.log',
'$path/$1/bad/AICS_SECURITY_MASTER.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  # ONLOAD TABLE AICS_SECURITY_POSITION
  echo "$T begin to load table AICS_SECURITY_POSITION"
  echo "[INFO] $T begin to load table AICS_SECURITY_POSITION">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_POSITION.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityPosition.csv\""" "$ctl_path"/control/AICS_SECURITY_POSITION.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_POSITION.ctl bad="$path/$1"/bad/AICS_SECURITY_POSITION.bad log="$path/$1"/log/AICS_SECURITY_POSITION.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_POSITION.bad ];then  
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
'$path/$1/csv/$1-SecurityPosition.csv',
'src_layer.AICS_SECURITY_POSITION',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_POSITION.log',
'$path/$1/bad/AICS_SECURITY_POSITION.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE AICS_SECURITY_POSITION_HIS
  echo "$T begin to load table AICS_SECURITY_POSITION_HIS"
  echo "[INFO] $T begin to load table AICS_SECURITY_POSITION_HIS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_POSITION_HIS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityPositionHis.csv\""" "$ctl_path"/control/AICS_SECURITY_POSITION_HIS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_POSITION_HIS.ctl bad="$path/$1"/bad/AICS_SECURITY_POSITION_HIS.bad log="$path/$1"/log/AICS_SECURITY_POSITION_HIS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_POSITION_HIS.bad ];then  
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
'$path/$1/csv/$1-SecurityPositionHis.csv',
'src_layer.AICS_SECURITY_POSITION_HIS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_POSITION_HIS.log',
'$path/$1/bad/AICS_SECURITY_POSITION_HIS.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE AICS_SECURITY_PRICE
  echo "$T begin to load table AICS_SECURITY_PRICE"
  echo "[INFO] $T begin to load table AICS_SECURITY_PRICE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_PRICE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityPrice.csv\""" "$ctl_path"/control/AICS_SECURITY_PRICE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_PRICE.ctl bad="$path/$1"/bad/AICS_SECURITY_PRICE.bad log="$path/$1"/log/AICS_SECURITY_PRICE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_PRICE.bad ];then  
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
'$path/$1/csv/$1-SecurityPrice.csv',
'src_layer.AICS_SECURITY_PRICE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_PRICE.log',
'$path/$1/bad/AICS_SECURITY_PRICE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE AICS_SECURITY_PRICE_HIS
  echo "$T begin to load table AICS_SECURITY_PRICE_HIS"
  echo "[INFO] $T begin to load table AICS_SECURITY_PRICE_HIS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_PRICE_HIS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityPriceHis.csv\""" "$ctl_path"/control/AICS_SECURITY_PRICE_HIS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_PRICE_HIS.ctl bad="$path/$1"/bad/AICS_SECURITY_PRICE_HIS.bad log="$path/$1"/log/AICS_SECURITY_PRICE_HIS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_PRICE_HIS.bad ];then  
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
'$path/$1/csv/$1-SecurityPriceHis.csv',
'src_layer.AICS_SECURITY_PRICE_HIS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_PRICE_HIS.log',
'$path/$1/bad/AICS_SECURITY_PRICE_HIS.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
   # ONLOAD TABLE AICS_SECURITY_RATE_ADJUST
  echo "$T begin to load table AICS_SECURITY_RATE_ADJUST"
  echo "[INFO] $T begin to load table AICS_SECURITY_RATE_ADJUST">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_RATE_ADJUST.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityRateAdjust.csv\""" "$ctl_path"/control/AICS_SECURITY_RATE_ADJUST.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_RATE_ADJUST.ctl bad="$path/$1"/bad/AICS_SECURITY_RATE_ADJUST.bad log="$path/$1"/log/AICS_SECURITY_RATE_ADJUST.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_RATE_ADJUST.bad ];then  
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
'$path/$1/csv/$1-SecurityRateAdjust.csv',
'src_layer.AICS_SECURITY_RATE_ADJUST',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_RATE_ADJUST.log',
'$path/$1/bad/AICS_SECURITY_RATE_ADJUST.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  

 # ONLOAD TABLE AICS_SECURITY_SCHEDULE
  echo "$T begin to load table AICS_SECURITY_SCHEDULE"
  echo "[INFO] $T begin to load table AICS_SECURITY_SCHEDULE">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_SCHEDULE.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecuritySchedule.csv\""" "$ctl_path"/control/AICS_SECURITY_SCHEDULE.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_SCHEDULE.ctl bad="$path/$1"/bad/AICS_SECURITY_SCHEDULE.bad log="$path/$1"/log/AICS_SECURITY_SCHEDULE.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_SCHEDULE.bad ];then  
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
'$path/$1/csv/$1-SecuritySchedule.csv',
'src_layer.AICS_SECURITY_SCHEDULE',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_SCHEDULE.log',
'$path/$1/bad/AICS_SECURITY_SCHEDULE.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
   # ONLOAD TABLE AICS_SECURITY_YIELD
  echo "$T begin to load table AICS_SECURITY_YIELD"
  echo "[INFO] $T begin to load table AICS_SECURITY_YIELD">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_SECURITY_YIELD.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-SecurityYield.csv\""" "$ctl_path"/control/AICS_SECURITY_YIELD.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_SECURITY_YIELD.ctl bad="$path/$1"/bad/AICS_SECURITY_YIELD.bad log="$path/$1"/log/AICS_SECURITY_YIELD.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_SECURITY_YIELD.bad ];then  
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
'$path/$1/csv/$1-SecurityYield.csv',
'src_layer.AICS_SECURITY_YIELD',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_SECURITY_YIELD.log',
'$path/$1/bad/AICS_SECURITY_YIELD.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
    
     # ONLOAD TABLE AICS_FUNDINFO
  echo "$T begin to load table AICS_FUNDINFO"
  echo "[INFO] $T begin to load table AICS_FUNDINFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_FUNDINFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-FundInfo.csv\""" "$ctl_path"/control/AICS_FUNDINFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_FUNDINFO.ctl bad="$path/$1"/bad/AICS_FUNDINFO.bad log="$path/$1"/log/AICS_FUNDINFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_FUNDINFO.bad ];then  
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
'$path/$1/csv/$1-FundInfo.csv',
'src_layer.AICS_FUNDINFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_FUNDINFO.log',
'$path/$1/bad/AICS_FUNDINFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi



  
     # ONLOAD TABLE AICS_CUSTOMER_INFO
  echo "$T begin to load table AICS_CUSTOMER_INFO"
  echo "[INFO] $T begin to load table AICS_CUSTOMER_INFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_CUSTOMER_INFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CustomerInfo.csv\""" "$ctl_path"/control/AICS_CUSTOMER_INFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_CUSTOMER_INFO.ctl bad="$path/$1"/bad/AICS_CUSTOMER_INFO.bad log="$path/$1"/log/AICS_CUSTOMER_INFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_CUSTOMER_INFO.bad ];then  
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
'$path/$1/csv/$1-CustomerInfo.csv',
'src_layer.AICS_CUSTOMER_INFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_CUSTOMER_INFO.log',
'$path/$1/bad/AICS_CUSTOMER_INFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi




  
     # ONLOAD TABLE AICS_TRUSTCONTRACTDETAILS
  echo "$T begin to load table AICS_TRUSTCONTRACTDETAILS"
  echo "[INFO] $T begin to load table AICS_TRUSTCONTRACTDETAILS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_TRUSTCONTRACTDETAILS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-Trustcontractdetails.csv\""" "$ctl_path"/control/AICS_TRUSTCONTRACTDETAILS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_TRUSTCONTRACTDETAILS.ctl bad="$path/$1"/bad/AICS_TRUSTCONTRACTDETAILS.bad log="$path/$1"/log/AICS_TRUSTCONTRACTDETAILS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_TRUSTCONTRACTDETAILS.bad ];then  
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
'$path/$1/csv/$1-Trustcontractdetails.csv',
'src_layer.AICS_TRUSTCONTRACTDETAILS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_TRUSTCONTRACTDETAILS.log',
'$path/$1/bad/AICS_TRUSTCONTRACTDETAILS.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi



  
     # ONLOAD TABLE AICS_STATICSHARES
  echo "$T begin to load table AICS_STATICSHARES"
  echo "[INFO] $T begin to load table AICS_STATICSHARES">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_STATICSHARES.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-Staticshares.csv\""" "$ctl_path"/control/AICS_STATICSHARES.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_STATICSHARES.ctl bad="$path/$1"/bad/AICS_STATICSHARES.bad log="$path/$1"/log/AICS_STATICSHARES.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_STATICSHARES.bad ];then  
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
'$path/$1/csv/$1-Staticshares.csv',
'src_layer.AICS_STATICSHARES',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_STATICSHARES.log',
'$path/$1/bad/AICS_STATICSHARES.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi


  
     # ONLOAD TABLE AICS_V_PROFITDEALCURRENTS
  echo "$T begin to load table AICS_V_PROFITDEALCURRENTS"
  echo "[INFO] $T begin to load table AICS_V_PROFITDEALCURRENTS">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_V_PROFITDEALCURRENTS.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-Vprofitdealcurrents.csv\""" "$ctl_path"/control/AICS_V_PROFITDEALCURRENTS.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_V_PROFITDEALCURRENTS.ctl bad="$path/$1"/bad/AICS_V_PROFITDEALCURRENTS.bad log="$path/$1"/log/AICS_V_PROFITDEALCURRENTS.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_V_PROFITDEALCURRENTS.bad ];then  
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
'$path/$1/csv/$1-Vprofitdealcurrents.csv',
'src_layer.AICS_V_PROFITDEALCURRENTS',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_V_PROFITDEALCURRENTS.log',
'$path/$1/bad/AICS_V_PROFITDEALCURRENTS.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi


  
     # ONLOAD TABLE AICS_V_FAREMITNEW
  echo "$T begin to load table AICS_V_FAREMITNEW"
  echo "[INFO] $T begin to load table AICS_V_FAREMITNEW">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_V_FAREMITNEW.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-Vfaremitnew.csv\""" "$ctl_path"/control/AICS_V_FAREMITNEW.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_V_FAREMITNEW.ctl bad="$path/$1"/bad/AICS_V_FAREMITNEW.bad log="$path/$1"/log/AICS_V_FAREMITNEW.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_V_FAREMITNEW.bad ];then  
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
'$path/$1/csv/$1-Vfaremitnew.csv',
'src_layer.AICS_V_FAREMITNEW',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_V_FAREMITNEW.log',
'$path/$1/bad/AICS_V_FAREMITNEW.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi

  
     # ONLOAD TABLE AICS_CONFIRM
  echo "$T begin to load table AICS_CONFIRM"
  echo "[INFO] $T begin to load table AICS_CONFIRM">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_CONFIRM.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-Confirm.csv\""" "$ctl_path"/control/AICS_CONFIRM.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_CONFIRM.ctl bad="$path/$1"/bad/AICS_CONFIRM.bad log="$path/$1"/log/AICS_CONFIRM.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_CONFIRM.bad ];then  
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
'$path/$1/csv/$1-Confirm.csv',
'src_layer.AICS_CONFIRM',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_CONFIRM.log',
'$path/$1/bad/AICS_CONFIRM.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log


  
  # ONLOAD TABLE AICS_CREDITOR_VARY_RELATION
  echo "$T begin to load table AICS_CREDITOR_VARY_RELATION"
  echo "[INFO] $T begin to load table AICS_CREDITOR_VARY_RELATION">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_CREDITOR_VARY_RELATION.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-CreditorVaryRelation.csv\""" "$ctl_path"/control/AICS_CREDITOR_VARY_RELATION.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_CREDITOR_VARY_RELATION.ctl bad="$path/$1"/bad/AICS_CREDITOR_VARY_RELATION.bad log="$path/$1"/log/AICS_CREDITOR_VARY_RELATION.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_CREDITOR_VARY_RELATION.bad ];then  
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
'$path/$1/csv/$1-CreditorVaryRelation.csv',
'src_layer.AICS_CREDITOR_VARY_RELATION',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_CREDITOR_VARY_RELATION.log',
'$path/$1/bad/AICS_CREDITOR_VARY_RELATION.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
     # ONLOAD TABLE AICS_INVEST_FUNDVARY
  echo "$T begin to load table AICS_INVEST_FUNDVARY"
  echo "[INFO] $T begin to load table AICS_INVEST_FUNDVARY">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_INVEST_FUNDVARY.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-InvestFundVary.csv\""" "$ctl_path"/control/AICS_INVEST_FUNDVARY.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_INVEST_FUNDVARY.ctl bad="$path/$1"/bad/AICS_INVEST_FUNDVARY.bad log="$path/$1"/log/AICS_INVEST_FUNDVARY.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_INVEST_FUNDVARY.bad ];then  
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
'$path/$1/csv/$1-InvestFundVary.csv',
'src_layer.AICS_INVEST_FUNDVARY',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_INVEST_FUNDVARY.log',
'$path/$1/bad/AICS_INVEST_FUNDVARY.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
     # ONLOAD TABLE AICS_INVEST_PROFITDETAIL
  echo "$T begin to load table AICS_INVEST_PROFITDETAIL"
  echo "[INFO] $T begin to load table AICS_INVEST_PROFITDETAIL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_INVEST_PROFITDETAIL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-InvestProfitDetail.csv\""" "$ctl_path"/control/AICS_INVEST_PROFITDETAIL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_INVEST_PROFITDETAIL.ctl bad="$path/$1"/bad/AICS_INVEST_PROFITDETAIL.bad log="$path/$1"/log/AICS_INVEST_PROFITDETAIL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_INVEST_PROFITDETAIL.bad ];then  
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
'$path/$1/csv/$1-InvestProfitDetail.csv',
'src_layer.AICS_INVEST_PROFITDETAIL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_INVEST_PROFITDETAIL.log',
'$path/$1/bad/AICS_INVEST_PROFITDETAIL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
     # ONLOAD TABLE AICS_REPAY_CONFIRM
  echo "$T begin to load table AICS_REPAY_CONFIRM"
  echo "[INFO] $T begin to load table AICS_REPAY_CONFIRM">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_REPAY_CONFIRM.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-RepayConfirm.csv\""" "$ctl_path"/control/AICS_REPAY_CONFIRM.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_REPAY_CONFIRM.ctl bad="$path/$1"/bad/AICS_REPAY_CONFIRM.bad log="$path/$1"/log/AICS_REPAY_CONFIRM.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_REPAY_CONFIRM.bad ];then  
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
'$path/$1/csv/$1-RepayConfirm.csv',
'src_layer.AICS_REPAY_CONFIRM',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_REPAY_CONFIRM.log',
'$path/$1/bad/AICS_REPAY_CONFIRM.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
     # ONLOAD TABLE AICS_REPAY_CONFIRM_DETAIL
  echo "$T begin to load table AICS_REPAY_CONFIRM_DETAIL"
  echo "[INFO] $T begin to load table AICS_REPAY_CONFIRM_DETAIL">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_REPAY_CONFIRM_DETAIL.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-RepayConfirmDetail.csv\""" "$ctl_path"/control/AICS_REPAY_CONFIRM_DETAIL.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_REPAY_CONFIRM_DETAIL.ctl bad="$path/$1"/bad/AICS_REPAY_CONFIRM_DETAIL.bad log="$path/$1"/log/AICS_REPAY_CONFIRM_DETAIL.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_REPAY_CONFIRM_DETAIL.bad ];then  
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
'$path/$1/csv/$1-RepayConfirmDetail.csv',
'src_layer.AICS_REPAY_CONFIRM_DETAIL',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_REPAY_CONFIRM_DETAIL.log',
'$path/$1/bad/AICS_REPAY_CONFIRM_DETAIL.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
     # ONLOAD TABLE AICS_REPAY_INFO
  echo "$T begin to load table AICS_REPAY_INFO"
  echo "[INFO] $T begin to load table AICS_REPAY_INFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_REPAY_INFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-TrepayInfo.csv\""" "$ctl_path"/control/AICS_REPAY_INFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_REPAY_INFO.ctl bad="$path/$1"/bad/AICS_REPAY_INFO.bad log="$path/$1"/log/AICS_REPAY_INFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_REPAY_INFO.bad ];then  
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
'$path/$1/csv/$1-TrepayInfo.csv',
'src_layer.AICS_REPAY_INFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_REPAY_INFO.log',
'$path/$1/bad/AICS_REPAY_INFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
  
    # ONLOAD TABLE AICS_DISPOSAL_OBJECTINFO
  echo "$T begin to load table AICS_DISPOSAL_OBJECTINFO"
  echo "[INFO] $T begin to load table AICS_DISPOSAL_OBJECTINFO">>"$path"/error.log
  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
  sed -i '3d' "$ctl_path"/control/AICS_DISPOSAL_OBJECTINFO.ctl 
  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
  sed -i "/LOAD DATA/aINFILE "\"$path/$1/csv/$1-DisposalObjectinfo.csv\""" "$ctl_path"/control/AICS_DISPOSAL_OBJECTINFO.ctl
  echo "************begin**************">>"$path"/error.log  
  # SQLLDR FOR LOADDATA
  sqlldr src_layer/oracle@ICBCAMCDW control="$ctl_path"/control/AICS_DISPOSAL_OBJECTINFO.ctl bad="$path/$1"/bad/AICS_DISPOSAL_OBJECTINFO.bad log="$path/$1"/log/AICS_DISPOSAL_OBJECTINFO.log>>"$path"/error.log
    # IS SUCCESS OR NOT 
  if [ -f "$path/$1"/bad/AICS_DISPOSAL_OBJECTINFO.bad ];then  
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
'$path/$1/csv/$1-DisposalObjectinfo.csv',
'src_layer.AICS_DISPOSAL_OBJECTINFO',
(select data_date from etl_control.ctl_general_parameter where rownum=1),
sysdate,
'$is_success',
'$path/$1/log/AICS_DISPOSAL_OBJECTINFO.log',
'$path/$1/bad/AICS_DISPOSAL_OBJECTINFO.bad');
commit;
exit;
EOF
  echo "*************end***************">>"$path"/error.log
  
else 
echo "$T the ready document is not exist ,please check whether the file is ready "
echo "[ERROR] $T startelt at  not exist $DOCUMENTNAME">>"$path"/error.log
fi


  
  
  
  