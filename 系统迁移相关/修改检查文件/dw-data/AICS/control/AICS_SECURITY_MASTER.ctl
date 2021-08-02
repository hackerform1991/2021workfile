OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-SecurityMaster.csv"
truncate
INTO TABLE AICS_SECURITY_MASTER
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  security            CHAR(32),
  name                CHAR(255),
  type                CHAR(32),
  ccy                 CHAR(3),
  par_value           ,
  price               ,
  issdate             DATE"YYYY-MM-DD HH24:MI:SS" ,
  vdate               DATE"YYYY-MM-DD HH24:MI:SS" ,
  mdate               DATE"YYYY-MM-DD HH24:MI:SS" ,
  ratecode            CHAR(8),
  fixrate             ,
  issuer              CHAR(32),
  master_desc         CHAR(300),
  status              CHAR(2),
  createuser          CHAR(32),
  createtime          DATE"YYYY-MM-DD HH24:MI:SS" ,
  lstmntuser          CHAR(32),
  lstmnttime          DATE"YYYY-MM-DD HH24:MI:SS" ,
  feno                CHAR(32),
  isuse               CHAR(1),
  intsecid            CHAR(32),
  marketissue         CHAR(32),
  custodian           CHAR(32),
  basis               CHAR(8),
  firstintpaydate     DATE"YYYY-MM-DD HH24:MI:SS" ,
  intpmtfreq          CHAR(3),
  intpayrule          CHAR(8),
  firstprinpayday     DATE"YYYY-MM-DD HH24:MI:SS" ,
  prinpmtfreq         CHAR(3),
  settdays            ,
  sic                 CHAR(32),
  sectype             CHAR(1),
  guarantee           CHAR(32),
  conversion_star     DATE"YYYY-MM-DD HH24:MI:SS" ,
  conversion_end      DATE"YYYY-MM-DD HH24:MI:SS" ,
  calldate            DATE"YYYY-MM-DD HH24:MI:SS" ,
  callprice           ,
  call_desc           CHAR(300),
  putbackdate         DATE"YYYY-MM-DD HH24:MI:SS" ,
  putbackprice        ,
  putback_desc        CHAR(300),
  linksecid           CHAR(32),
  listedsector        CHAR(8),
  rateadjustdate      DATE"YYYY-MM-DD HH24:MI:SS" ,
  category            CHAR(32),
  fund_manager        CHAR(32),
  trustee             CHAR(32),
  prinpmt_type        CHAR(3),
  fullname            CHAR(200),
  source              CHAR(32),
  verifyuser          CHAR(32),
  verifytime          DATE"YYYY-MM-DD HH24:MI:SS" ,
  auto_verify         CHAR(2),
  reject_remark       CHAR(255),
  domestic            CHAR(3),
  totalissue          ,
  a_totalshare        ,
  a_totalfloatshare   ,
  h_totalshare        ,
  h_totalfloatshare   ,
  area                CHAR(50),
  rejectuser          CHAR(32),
  rejecttime          DATE"YYYY-MM-DD HH24:MI:SS" ,
  pubpvt              CHAR(8),
  name_allfirstletter CHAR(32),
  industrywind        CHAR(32),
  mkt_security        CHAR(32),
  price_client_code   CHAR(32),
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
