OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-InvestFloatRate.csv"
truncate
INTO TABLE AICS_INVEST_FLOATRATE
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(

  contractid ,
  assetid    ,
  rateid     ,
  ratekind   ,
  begindate  DATE"YYYY-MM-DD HH24:MI:SS" ,
  enddate    DATE"YYYY-MM-DD HH24:MI:SS" ,
  beginscale ,
  endscale   ,
  rate       ,
  calccycle  ,
  ratedays   ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)

