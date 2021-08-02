OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-AssetLoanaccount.csv"
truncate
INTO TABLE AICS_ASSET_LOANACCOUNT
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  projectcode       ,
  subprojectcode    ,
  loanaccountcode   ,
  init_balance      ,
  current_balance   ,
  frozen_balance    ,
  unfrozen_balance  ,
  current_assetcost ,
  moneytype         ,
  createuser        ,
  createtime         DATE"YYYY-MM-DD HH24:MI:SS" ,
  updateuser        ,
  updatetime         DATE"YYYY-MM-DD HH24:MI:SS" ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
