OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-InvestPlanDetail.csv"
truncate
INTO TABLE AICS_INVEST_PLANDETAIL
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
   pkid      ,
  planid     ,
  contractid ,
  assetid    ,
  pdate       DATE"YYYY-MM-DD HH24:MI:SS",
  profitkind  ,
  extflag     ,
  balance     ,
  invest      ,
  rateid      ,
  bdate       DATE"YYYY-MM-DD HH24:MI:SS",
  edate       DATE"YYYY-MM-DD HH24:MI:SS",
  settldate   DATE"YYYY-MM-DD HH24:MI:SS",
  rate        ,
  repayflag   ,
  repaybal    ,
  lastrepay   DATE"YYYY-MM-DD HH24:MI:SS",
  days        ,
  ratedays    ,
  assetcode   ,
  autobalance ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
