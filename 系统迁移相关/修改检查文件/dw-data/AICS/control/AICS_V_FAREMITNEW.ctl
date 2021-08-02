OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190307/csv/20190307-InvestPlan.csv"
truncate
INTO TABLE AICS_INVEST_PLAN
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 
  serialno      CHAR(44),
  projectcode   CHAR(255),
  fundcode      CHAR(6),
  fundacco      CHAR(16),
  businflag     CHAR(2),
  occurbalance  ,
  arrivedate    DATE"YYYY-MM-DD HH24:MI:SS",
  isreplenish   CHAR(1),
  bankacco      CHAR(100),
  isback        CHAR(1),
  posfare       ,
  hkph          CHAR(48),
  isbonusrtnpri CHAR(1),
  moneytype     CHAR(3),
  d_date        DATE"YYYY-MM-DD HH24:MI:SS",
  fromaccoid    CHAR(6),
  toaccoid      CHAR(6),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
