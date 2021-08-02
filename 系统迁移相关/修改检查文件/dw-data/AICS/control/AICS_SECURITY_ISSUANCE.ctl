OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-SecurityIssuance.csv"
truncate
INTO TABLE AICS_SECURITY_ISSUANCE
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  br              CHAR(6),
  security        CHAR(32),
  name            CHAR(255),
  ccy             CHAR(3),
  par_value       ,
  price           ,
  qtyissued       ,
  amtissued       ,
  oid             ,
  ratecode        CHAR(8),
  fixrate         ,
  intpmtfreq      CHAR(3),
  issdate         DATE"YYYY-MM-DD HH24:MI:SS" ,
  vdate           DATE"YYYY-MM-DD HH24:MI:SS" ,
  tenor           CHAR(6),
  tenor_unit      CHAR(2),
  firstintpaydate DATE"YYYY-MM-DD HH24:MI:SS" ,
  mdate           DATE"YYYY-MM-DD HH24:MI:SS" ,
  mtyreftype      CHAR(32),
  domestic        CHAR(3),
  marketissue     CHAR(8),
  createuser      CHAR(32),
  createtime      DATE"YYYY-MM-DD HH24:MI:SS" ,
  lstmntuser      CHAR(32),
  lstmnttime      DATE"YYYY-MM-DD HH24:MI:SS" ,
  status          CHAR(2),
  type            CHAR(6),
  basis           CHAR(8),
  custcode        CHAR(32),
  account         CHAR(32),
  remark          CHAR(3000),
  buis_module     CHAR(32),
  tempcode        CHAR(20),
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
