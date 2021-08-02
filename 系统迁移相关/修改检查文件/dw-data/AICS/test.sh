path="/dw-data/AICS"

 #truncate table EBS_DATA_HEADER
 sqlplus src_layer/oracle@ICBCAMCDW<<EOF
set linesize 200
set pagesize 200
truncate table AICS_DATA_HEADER;
commit;
exit;
EOF
 
 
# for file in $path/testcsv/* ;
# do  
#  #get  document name and period 
#  echo $file
#  #get filename
#  temp=${file##*/}
#  filename=${temp%.*}
#  echo filename is $filename  
#  echo "[INFO] $T $file is begining"
#  period=${file:0-10:6}
#  echo $period
#  echo "$T begin to load table  AICS_DATA_HEADER"
#  # UPDATE THE PROPERTY CSV DOCUMENT TO ADAPT HAVING DOCUMENT
#  sed -i '3d' "$path"/control/AICS_DATA_HEADER.ctl 
#  # INSERT TRUE CONFIG FOR CONTROL DOCUMENT
#  sed -i "/LOAD DATA/aINFILE "\"$file\""" "$path"/control/AICS_DATA_HEADER.ctl 
#  # SQLLDR FOR LOADDATA
#  sqlldr src_layer/oracle@ICBCAMCDW control="$path"/control/AICS_DATA_HEADER.ctl bad="$path"/"$filename"_HEADER.bad log="$path"/"$filename"_HEADER.log
#  done
  

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_ACCOUNT_BANKINFO.ctl" log=bankinfo.log bad=backinfo.bad
 
sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_ASSET_LOANACCOUNT_INFO.ctl" log=loanaccountinfo.log bad=loanaccountinfo.bad

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_CREDITOR_VARY_RELATION.ctl" log=creditorveryrelation.log bad=creditorveryrelation.bad

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_CUSTOMER_INFO.ctl" log=customer.log bad=customer.bad

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_EQUITY_ASSETS_INFO.ctl" log=equity_assets_info.log bad=equity_assets_info.bad

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_INVEST_CONTRACT.ctl" log=invest_contract.log bad=invest_contract.bad

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_INVEST_FUNDVARY.ctl" log=invest_fundvary.log bad=invest_fundvary.bad

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_INVEST_PROFITDETAIL.ctl" log=invest_profitdetail.log bad=invest_profitdetail.bad

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_REPAY_INFO.ctl" log=repay_info.log bad=repay_info.bad

sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_PROJECT_INFO.ctl" log=project_info.log bad=project_info.bad



sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_ASSET_CREDITOR_RELATION.ctl" log=asset_creditor_relation.log bad=asset_creditor_relation.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_ASSET_CREDITORCONTRACT.ctl" log=asset_creditorcontract.log bad=asset_creditorcontract.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_ASSET_GUARANTEECONTRACT.ctl" log=asset_guaranteecontract.log bad=asset_guaranteecontract.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_ASSET_LOANACCOUNT.ctl" log=asset_loanaccount.log bad=asset_loanaccount.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_ASSET_MORTGAGECONTRACT.ctl" log=asset_mortgagecontract.log bad=asset_mortgagecontract.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_ASSET_PAWN_INFO.ctl" log=asset_pawn_info.log bad=asset_pawn_info.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_INVEST_FLOATRATE.ctl" log=invest_floatrate.log bad=invest_floatrate.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_INVEST_PLANDETAIL.ctl" log=invest_plandetail.log bad=invest_plandetail.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_INVEST_PLAN.ctl" log=invest_plan.log bad=invest_plan.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_INVEST_RATE.ctl" log=invest_rate.log bad=invest_rate.bad


sqlldr src_layer/oracle@ICBCAMCDW control="/dw-data/AICS/control/AICS_REPAY_CONFIRM.ctl" log=repay_confirm.log bad=repay_confirm.bad



echo "complate"
