OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-FundInfo.csv"
truncate
INTO TABLE AICS_INVEST_PLAN
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 
  fundcode        CHAR(6),
  fundname        CHAR(255),
  fundshortname   CHAR(60),
  projectcode     CHAR(255),
  subprojectcode  CHAR(60),
  moneytype       CHAR(3),
  issueprice      ,
  issuedate       DATE"YYYY-MM-DD HH24:MI:SS",
  setupdate       DATE"YYYY-MM-DD HH24:MI:SS",
  fundstatus      CHAR(1),
  timelimit       ,
  issueenddate    DATE"YYYY-MM-DD HH24:MI:SS",
  contractenddate DATE"YYYY-MM-DD HH24:MI:SS",
  structflag      CHAR(1),
  structtype      CHAR(1),
  riskgrade       CHAR(2),
  funddesc        CHAR(255),
  timelimitunit   CHAR(5),
  minbala         ,
  maxbala         ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
