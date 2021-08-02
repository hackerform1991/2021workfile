OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-SecurityYield.csv"
truncate
INTO TABLE AICS_SECURITY_YIELD
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  dealno          CHAR(32),
  security        CHAR(32),
  yieldtype       CHAR(4),
  dealdate        DATE"YYYY-MM-DD HH24:MI:SS" ,
  prinamt         ,
  quantity        ,
  price           ,
  rate            ,
  basis           CHAR(8),
  intpmtfreq      CHAR(3),
  ccy             CHAR(3),
  yield_amt       ,
  startdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  enddate         DATE"YYYY-MM-DD HH24:MI:SS" ,
  costcentertype  CHAR(32),
  costcenter      CHAR(32),
  auto_verify     CHAR(1),
  status          CHAR(1),
  posflag         CHAR(32),
  fedealno        CHAR(64),
  createuser      CHAR(32),
  createtime      DATE"YYYY-MM-DD HH24:MI:SS" ,
  verifyuser      CHAR(32),
  verifytime      DATE"YYYY-MM-DD HH24:MI:SS" ,
  reversaluser    CHAR(32),
  reversaltime    DATE"YYYY-MM-DD HH24:MI:SS" ,
  reversal_status CHAR(1),
  reversal_remark CHAR(200),
  reject_remark   CHAR(255),
  fee             ,
  par_value       ,
  acup_postdate   CHAR(32),
  custcode        CHAR(32),
  br              CHAR(32),
  settlement_path CHAR(32),
  buis_module     CHAR(32),
  product         CHAR(32),
  prodtype        CHAR(32),
  faceamount      ,
  auto_settlement CHAR(1),
  marketissue     CHAR(8),
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
