OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190226-AICS/20190226-AccountBankinfo.csv"
truncate
INTO TABLE AICS_DL_DEAL_ORDER
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  br             CHAR(32),
  orderno        CHAR(32),
  order_headno   CHAR(32),
  buis_module    CHAR(32),
  costcentertype CHAR(6),
  costcenter     CHAR(32),
  product        CHAR(32),
  prodtype       CHAR(32),
  dealdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  dealdate_end   DATE"YYYY-MM-DD HH24:MI:SS" ,
  dl_ps          CHAR(2),
  cust           CHAR(32),
  vdate          DATE"YYYY-MM-DD HH24:MI:SS" ,
  tenor          ,
  tenor_unit     CHAR(2),
  mdate          DATE"YYYY-MM-DD HH24:MI:SS" ,
  ccy            CHAR(32),
  amt            ,
  intrate        ,
  deal_desc      CHAR(1000),
  status         CHAR(2),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
