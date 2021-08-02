OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-Customerinfo.csv"
truncate
INTO TABLE AICS_INVEST_PLAN
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 
  fundacco     CHAR(12),
  custname     CHAR(160),
  custtype     CHAR(1),
  identitytype CHAR(1),
  identityno   CHAR(50),
  sex          CHAR(1),
  address      CHAR(255),
  mobileno     CHAR(40),
  phone        CHAR(25),
  email        CHAR(40),
  riskgrade    CHAR(2),
  appenddate   DATE"YYYY-MM-DD HH24:MI:SS",
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
