OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-SecurityPrice.csv"
truncate
INTO TABLE AICS_SECURITY_PRICE
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  security          CHAR(32),
  effective_date    DATE"YYYY-MM-DD HH24:MI:SS" ,
  closing_price     ,
  dirty_price       ,
  closing_ask_price ,
  closing_bid_price ,
  lstmntuser        CHAR(32),
  lstmnttime        DATE"YYYY-MM-DD HH24:MI:SS" ,
  preclosing_price  ,
  open_price        ,
  daily_profit      ,
  max_price         ,
  min_price         ,
  l_turnover        ,
  turnover_amt      ,
  unitnv            ,
  latestweeklyyield ,
  category          CHAR(32),
  net_change        ,
  change            ,
  f_5ma             ,
  f_20ma            ,
  marketissue       CHAR(8),
  ytm               ,
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
