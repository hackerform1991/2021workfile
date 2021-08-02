OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190226-AICS/20190226-AccountBankinfo.csv"
truncate
INTO TABLE AICS_REMITMONEYDETAIL
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  serialno             ,
  parentserialno       ,
  batchserialno        ,
  firstserialno        ,
  balance              ,
  paiddate             DATE"YYYY-MM-DD HH24:MI:SS" ,
  expectdate           DATE"YYYY-MM-DD HH24:MI:SS" ,
  status               CHAR(1),
  flowstatus           CHAR(1),
  flowenddate          DATE"YYYY-MM-DD HH24:MI:SS" ,
  seq                  ,
  submitstatus         CHAR(1),
  frombankno           CHAR(6),
  frombankname         CHAR(150),
  fromnameinbank       CHAR(300),
  frombankacco         CHAR(100),
  tobankno             CHAR(6),
  tobankname           CHAR(150),
  tonameinbank         CHAR(300),
  tobankacco           CHAR(100),
  direction            CHAR(2),
  backflag             CHAR(1),
  backdate             DATE"YYYY-MM-DD HH24:MI:SS" ,
  actualmoneyflag      CHAR(1),
  cause                CHAR(255),
  memo                 CHAR(255),
  othertransserialno   CHAR(10),
  fromcityno           CHAR(4),
  fromprovincecode     CHAR(3),
  tocityno             CHAR(4),
  toprovincecode       CHAR(3),
  frombanklinecode     CHAR(20),
  fromsubaccoid        CHAR(6),
  tobanklinecode       CHAR(20),
  moneytype            CHAR(3),
  realbalance          ,
  sumbitexportdate     DATE"YYYY-MM-DD HH24:MI:SS" ,
  statuslastmodifydate DATE"YYYY-MM-DD HH24:MI:SS" ,
  fromaccoid           CHAR(6),
  toaccoid             CHAR(6),
  outbatchserialno     ,
  nextdealflag         CHAR(1),
  remark               CHAR(1000),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
