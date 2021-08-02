OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190226-AICS/20190226-AccountBankinfo.csv"
truncate
INTO TABLE AICS_REPO_DEAL_ORDER
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  br             CHAR(32),
  orderno        CHAR(32),
  order_headno   CHAR(32),
  buis_module    CHAR(32),
  dealdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  end_dealdate   DATE"YYYY-MM-DD HH24:MI:SS" ,
  market         CHAR(8),
  product        CHAR(32),
  ps             CHAR(5),
  security       CHAR(32),
  quantity       ,
  ccy            CHAR(3),
  tenor          ,
  amount         ,
  amount_max     ,
  reporate       ,
  reporate_max   ,
  costcentertype CHAR(32),
  costcenter     CHAR(32),
  status         CHAR(1),
  createuser     CHAR(32),
  createtime     DATE"YYYY-MM-DD HH24:MI:SS" ,
  verifyuser     CHAR(32),
  verifytime     DATE"YYYY-MM-DD HH24:MI:SS" ,
  lstmntuser     CHAR(32),
  lstmnttime     DATE"YYYY-MM-DD HH24:MI:SS" ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
