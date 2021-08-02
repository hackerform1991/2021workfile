OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-InvestPlan.csv"
truncate
INTO TABLE AICS_INVEST_PLAN
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 
  planid         ,
  pdate          DATE"YYYY-MM-DD HH24:MI:SS",
  state          ,
  dataflag       ,
  create_date    DATE"YYYY-MM-DD HH24:MI:SS",
  creater        ,
  update_date    DATE"YYYY-MM-DD HH24:MI:SS",
  updater        ,
  equityplankind ,
  equityassetno  ,
  reviewstatus   ,
  bdate          DATE"YYYY-MM-DD HH24:MI:SS",
  edate          DATE"YYYY-MM-DD HH24:MI:SS",
  total_amount   ,
  projectcode    ,
  contractid     ,
  assetid        ,
  plantype       ,
  repayway       ,
  pcapital       ,
  fundacid       ,
  rivalid        ,
  accoid         ,
  pincome        ,
  mcapital       ,
  mincome        ,
  isrecreate     ,
  loanbalance    ,
  contractstate  ,
  repaymenttype  ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
