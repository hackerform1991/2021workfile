OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-DepositsLoansDeal.csv"
truncate
INTO TABLE AICS_DEPOSITS_LOANS_DEAL
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
   br              CHAR(8),
  dealno          CHAR(32),
  buis_module     CHAR(32),
  cost            CHAR(32),
  product         CHAR(32),
  prodtype        CHAR(32),
  dl_ps           CHAR(2),
  dealdate        DATE"YYYY-MM-DD HH24:MI:SS" ,
  vdate           DATE"YYYY-MM-DD HH24:MI:SS" ,
  tenor           ,
  tenor_unit      CHAR(2),
  mdate           DATE"YYYY-MM-DD HH24:MI:SS" ,
  cust            CHAR(32),
  cust_acct       CHAR(32),
  ccy             CHAR(32),
  amt             ,
  ratecode        CHAR(32),
  intrate         ,
  int_amt         ,
  basis           CHAR(8),
  acrfrstlst      CHAR(1),
  dealtext        CHAR(1000),
  status          CHAR(2),
  reversal_status CHAR(2),
  trad            CHAR(32),
  createuser      CHAR(32),
  createtime      DATE"YYYY-MM-DD HH24:MI:SS" ,
  reviewuser      CHAR(32),
  reviewtime      DATE"YYYY-MM-DD HH24:MI:SS" ,
  reversaluser    CHAR(32),
  reversaltime    DATE"YYYY-MM-DD HH24:MI:SS" ,
  lstmntuser      CHAR(32),
  lstmnttime      DATE"YYYY-MM-DD HH24:MI:SS" ,
  fedealno        CHAR(32),
  settlement_path CHAR(32),
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
