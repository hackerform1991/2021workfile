OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-SecuritySchedule.csv"
truncate
INTO TABLE AICS_SECURITY_SCHEDULE
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  schdid         CHAR(64),
  security       CHAR(32),
  seq            CHAR(10),
  schd_type      CHAR(3),
  paydate        DATE"YYYY-MM-DD HH24:MI:SS" ,
  cashflow       ,
  exdivdate      DATE"YYYY-MM-DD HH24:MI:SS" ,
  intpayamt      ,
  prinpayamt     ,
  ccy            CHAR(3),
  startdate      DATE"YYYY-MM-DD HH24:MI:SS" ,
  enddate        DATE"YYYY-MM-DD HH24:MI:SS" ,
  ratecode       CHAR(8),
  exercisedate   DATE"YYYY-MM-DD HH24:MI:SS" ,
  ratefixdate    DATE"YYYY-MM-DD HH24:MI:SS" ,
  floorrate      ,
  spreadrate     ,
  prinamt        ,
  basis          CHAR(8),
  steprate       ,
  notprinamt     ,
  notprinpayamt  ,
  notintpayamt   ,
  update_counter ,
  costcentertype CHAR(32),
  costcenter     CHAR(32),
  fedealno       CHAR(64),
  l_quantity     ,
  fixrate        ,
  isprocessed    CHAR(1),
  br             CHAR(32),
  buis_module    CHAR(32),
  product        CHAR(32),
  prodtype       CHAR(32),
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
