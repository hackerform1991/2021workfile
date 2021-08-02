OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190307/csv/20190307-InvestPlan.csv"
truncate
INTO TABLE AICS_INVEST_PLAN
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 
   businflag        CHAR(2),
  cdate            DATE"YYYY-MM-DD HH24:MI:SS",
  cserialno        CHAR(20),
  d_date           DATE"YYYY-MM-DD HH24:MI:SS",
  agencyno         CHAR(3),
  netno            CHAR(9),
  fundacco         CHAR(12),
  fundcode         CHAR(6),
  confirmbalance   ,
  confirmshares    ,
  tradefare        ,
  tafare           ,
  interest         ,
  agencyfare       ,
  netvalue         ,
  status           CHAR(1),
  cause            CHAR(500),
  interestshare    ,
  serialno         ,
  registfare       ,
  fundfare         ,
  contractserialno ,
  improperredeem   CHAR(1),
  taagencyfare     ,
  taregisterfare   ,
  netvaluedate     DATE"YYYY-MM-DD HH24:MI:SS",
  profitserialno   ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
