OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-SecurityPosition.csv"
truncate
INTO TABLE AICS_SECURITY_POSITION
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
  td_intamt          ,
  pye_intamt         ,
  td_salegain        ,
  pye_salegain       ,
  td_profit          ,
  pye_profit         ,
  yield_quantity     ,
  amortizedamt       ,
  unamortizedamt     ,
  frozent_qty        ,
  pye_yield_quantity ,
  br                 CHAR(32),
  buis_module        CHAR(32),
  product            CHAR(32),
  prodtype           CHAR(32),
  ccy                CHAR(4),
  feeamt             ,
  finance_principal  ,
  entrydate          DATE"YYYY-MM-DD HH24:MI:SS" ,
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
