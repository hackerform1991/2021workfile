OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190307/csv/20190307-InvestPlan.csv"
truncate
INTO TABLE AICS_INVEST_PLAN
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 
  projectcode       CHAR(60),
  subprojectcode    CHAR(60),
  fundcode          CHAR(6),
  contractserialno  ,
  fundacco          CHAR(16),
  custname          CHAR(160),
  registerdate      DATE"YYYY-MM-DD HH24:MI:SS",
  repaycapital      ,
  curbenefitcapital ,
  profit            ,
  subinterest       ,
  liqinterest       ,
  profitserialno    ,
  isplanningschema  CHAR(1),
  planningserialno  ,
  dealflag          CHAR(1),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
