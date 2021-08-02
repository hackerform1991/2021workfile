OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-InvestContract.csv"
truncate
INTO TABLE AICS_INVEST_CONTRACT
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  contractid          ,
  projectcode         ,
  rivalid             ,
  contractno          ,
  contractname        ,
  signdate            DATE"YYYY-MM-DD HH24:MI:SS" ,
  begdate             DATE"YYYY-MM-DD HH24:MI:SS" ,
  prestopdate         DATE"YYYY-MM-DD HH24:MI:SS" ,
  cykind              ,
  contractbalance     ,
  industry            ,
  stcokamount         ,
  investratio         ,
  isrepurchase        ,
  remark              char(4000),
  repurchaser         ,
  extendillustration2 char(3000),
  stock_price         ,
  investfundname      ,
  interest            ,
  stopinterestday     DATE"YYYY-MM-DD HH24:MI:SS" ,
  startcountday       DATE"YYYY-MM-DD HH24:MI:SS" ,
  debtamount          ,
  accrualtaxrate      ,
  startorg            ,
  ischeckbycondition  ,
  checkcondition      char(4000),
  maininvestpart      ,
  isenjoyvoteright    ,
  issendhighsup       ,
  assetrate           ,
  transferor          ,
  dealtype            ,
  purchasebanlance    ,
  transfermoney       ,
  paymenttype         ,
  fxassetrate         ,
  otherremark         char(4000),
  profitlevel         ,
  investplantype      ,
  underdescr          char(2000),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
