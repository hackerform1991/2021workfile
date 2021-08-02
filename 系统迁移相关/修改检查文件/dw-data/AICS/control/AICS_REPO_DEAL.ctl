OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-RepoDeal.csv"
truncate
INTO TABLE AICS_REPO_DEAL
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  br               CHAR(32),
  dealno           CHAR(32),
  buis_module      CHAR(8),
  trader           CHAR(32),
  market           CHAR(8),
  product          CHAR(32),
  security         CHAR(32),
  ps               CHAR(5),
  ratecode         CHAR(5),
  reporate         ,
  cust             CHAR(32),
  ccy              CHAR(3),
  quantity         ,
  faceamount       ,
  amount           ,
  totlint          ,
  dealdate         DATE"YYYY-MM-DD HH24:MI:SS" ,
  settdays         ,
  vdate            DATE"YYYY-MM-DD HH24:MI:SS" ,
  tenor            ,
  mdate            DATE"YYYY-MM-DD HH24:MI:SS" ,
  basis            CHAR(8),
  feeccy           CHAR(3),
  feeamt           ,
  settle_acct_type CHAR(3),
  settle_acct      CHAR(32),
  v_settlement     CHAR(3),
  m_settlement     CHAR(3),
  costcenter       CHAR(32),
  status           CHAR(1),
  mstatus          CHAR(1),
  reversal_status  CHAR(1),
  fedealno         CHAR(32),
  createuser       CHAR(32),
  createtime       DATE"YYYY-MM-DD HH24:MI:SS" ,
  reviewuser       CHAR(32),
  reviewtime       DATE"YYYY-MM-DD HH24:MI:SS" ,
  reversaluser     CHAR(32),
  reversaltime     DATE"YYYY-MM-DD HH24:MI:SS" ,
  reversareason    CHAR(100),
  lstmntuser       CHAR(32),
  lstmnttime       DATE"YYYY-MM-DD HH24:MI:SS" ,
  settlement_path  CHAR(32),
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
