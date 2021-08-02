OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190307/csv/20190307-InvestPlan.csv"
truncate
INTO TABLE AICS_INVEST_PLAN
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 
  serialno        ,
  fundcode        CHAR(6),
  fundacco        CHAR(12),
  tradeacco       CHAR(17),
  clientinfoid    CHAR(10),
  trustcontractid CHAR(100),
  operator        CHAR(32),
  inputdate       DATE"YYYY-MM-DD HH24:MI:SS",
  auditor         CHAR(32),
  auditdate       DATE"YYYY-MM-DD HH24:MI:SS",
  balance         ,
  sourcetype      CHAR(1),
  status          CHAR(1),
  profitclass     CHAR(10),
  bankno          CHAR(6),
  banklinecode    CHAR(20),
  bankname        CHAR(150),
  nameinbank      CHAR(300),
  bankacco        CHAR(100),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
