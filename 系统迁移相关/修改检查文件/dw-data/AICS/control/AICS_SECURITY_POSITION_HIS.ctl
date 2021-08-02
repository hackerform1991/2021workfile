OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-SecurityPositionHis.csv"
truncate
INTO TABLE AICS_SECURITY_POSITION_HIS
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  security           CHAR(32),
  quantity           ,
  average_cost       ,
  principal          ,
  profit             ,
  purch_avg_cost     ,
  purch_quantity     ,
  purch_amount       ,
  sale_avg_cost      ,
  sale_quantity      ,
  sale_amount        ,
  costcentertype     CHAR(32),
  costcenter         CHAR(32),
  closing_price      ,
  marketprice        ,
  postdate           DATE"YYYY-MM-DD HH24:MI:SS" ,
  td_intamt          ,
  pye_intamt         ,
  td_salegain        ,
  pye_salegain       ,
  td_profit          ,
  pye_profit         ,
  ccy                CHAR(3),
  yield_quantity     ,
  amortizedamt       ,
  unamortizedamt     ,
  br                 CHAR(32),
  buis_module        CHAR(32),
  product            CHAR(32),
  prodtype           CHAR(32),
  feeamt             ,
  finance_principal  ,
  profit_ratio       ,
  entrydate          DATE"YYYY-MM-DD HH24:MI:SS" ,
  insto              ,
  pye_yield_quantity ,
  floatprofit        ,
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
