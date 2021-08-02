OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-InvestRate.csv"
truncate
INTO TABLE AICS_INVEST_RATE
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  rateid       ,
  contractid   ,
  assetid      ,
  ratekind     ,
  cykind       ,
  calccycle    ,
  ratetype     ,
  bdate        DATE"YYYY-MM-DD HH24:MI:SS",
  edate        DATE"YYYY-MM-DD HH24:MI:SS",
  beginsacle   ,
  endsacle     ,
  basekind     ,
  floatcond    ,
  floatway     ,
  changekind   ,
  changefreq   ,
  changedate   DATE"YYYY-MM-DD HH24:MI:SS",
  lowrate      ,
  highrate     ,
  rate         ,
  settlefreq   ,
  isadvance    ,
  firstpaydate DATE"YYYY-MM-DD HH24:MI:SS",
  nextbdate    DATE"YYYY-MM-DD HH24:MI:SS",
  nextedate    DATE"YYYY-MM-DD HH24:MI:SS",
  nextpaydate  DATE"YYYY-MM-DD HH24:MI:SS",
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
