OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190226-AICS/20190226-AccountBankinfo.csv"
truncate
INTO TABLE AICS_SECURITY_DEAL_ORDER_HEAD
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  br                 CHAR(32),
  order_headno       CHAR(32),
  buis_module        CHAR(6),
  dealdate           DATE"YYYY-MM-DD HH24:MI:SS" ,
  end_dealdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  t_desc             CHAR(3000),
  status             CHAR(2),
  process_instanceid CHAR(100),
  createuser         CHAR(32),
  createtime         DATE"YYYY-MM-DD HH24:MI:SS" ,
  lstmntuser         CHAR(32),
  lstmnttime         DATE"YYYY-MM-DD HH24:MI:SS" ,
  costcenter         CHAR(32),
  costcentertype     CHAR(32),
  product            CHAR(32),
  prodtype           CHAR(32),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
