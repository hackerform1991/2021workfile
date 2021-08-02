OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190226-AICS/20190226-AccountBankinfo.csv"
truncate
INTO TABLE AICS_SECURITY_PERMIT_LIST
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  id          CHAR(100),
  security    CHAR(32),
  type        CHAR(32),
  br          CHAR(32),
  ispermit    CHAR(32),
  vdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  enddate     DATE"YYYY-MM-DD HH24:MI:SS" ,
  status      CHAR(3),
  createuser  CHAR(32),
  createdate  DATE"YYYY-MM-DD HH24:MI:SS" ,
  lstmntuser  CHAR(32),
  lstmnttime  DATE"YYYY-MM-DD HH24:MI:SS" ,
  busi_module CHAR(32),
  marketissue CHAR(8),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
