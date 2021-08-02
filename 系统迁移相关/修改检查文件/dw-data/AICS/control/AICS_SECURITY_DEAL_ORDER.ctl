OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190226-AICS/20190226-AccountBankinfo.csv"
truncate
INTO TABLE AICS_SECURITY_DEAL_ORDER
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  orderno        CHAR(32),
  projecttype    CHAR(32),
  investtype     CHAR(32),
  investtype2    CHAR(32),
  ps             CHAR(1),
  security       CHAR(32),
  quantity       ,
  ccy            CHAR(3),
  amount         ,
  custcode       CHAR(32),
  dealdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  costcentertype CHAR(32),
  costcenter     CHAR(32),
  status         CHAR(1),
  createuser     CHAR(32),
  createtime     DATE"YYYY-MM-DD HH24:MI:SS" ,
  verifyuser     CHAR(32),
  verifytime     DATE"YYYY-MM-DD HH24:MI:SS" ,
  price          ,
  fixrate        ,
  br             CHAR(32),
  buis_module    CHAR(32),
  order_headno   CHAR(32),
  t_desc         CHAR(3000),
  end_dealdate   DATE"YYYY-MM-DD HH24:MI:SS" ,
  price_max      ,
  amount_max     ,
  dealdate_max   DATE"YYYY-MM-DD HH24:MI:SS" ,
  quantity_max   ,
  marketissue    CHAR(32),
  tempcode       CHAR(20),
  ytm            ,
  ytm_max        ,
  prinamt        ,
  prinamt_max    ,
  faceamount     ,
  faceamount_max ,
  full_price     ,
  full_price_max ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
