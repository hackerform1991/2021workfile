OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190307/csv/20190307-InvestPlan.csv"
truncate
INTO TABLE AICS_INVEST_PLAN
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 
  fundacco         CHAR(12),
  fundcode         CHAR(6),
  agencyno         CHAR(3),
  realshares       ,
  frozenshares     ,
  lastshares       ,
  lastmodify       DATE"YYYY-MM-DD HH24:MI:SS",
  contractserialno ,
  updatedatetime   CHAR(25),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
