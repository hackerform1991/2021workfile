OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-SecurityRateAdjust.csv"
truncate
INTO TABLE AICS_SECURITY_RATE_ADJUST
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  security          CHAR(32),
  dealdate          DATE"YYYY-MM-DD HH24:MI:SS" ,
  fixrate           ,
  intpmtfreq        CHAR(3),
  adj_fixrate       ,
  is_adj_intpmtfreq CHAR(1),
  adj_intpmtfreq    CHAR(3),
  eff_rule          CHAR(1),
  effdate           DATE"YYYY-MM-DD HH24:MI:SS" ,
  createuser        CHAR(32),
  createtime        DATE"YYYY-MM-DD HH24:MI:SS" ,
  lstmntuser        CHAR(32),
  lstmnttime        DATE"YYYY-MM-DD HH24:MI:SS" ,
  adj_rateid        CHAR(40),
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
