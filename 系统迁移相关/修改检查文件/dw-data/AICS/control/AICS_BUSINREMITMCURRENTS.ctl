OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190226-AICS/20190226-AccountBankinfo.csv"
truncate
INTO TABLE AICS_BUSINREMITMCURRENTS
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  serialno         ,
  projectcode      CHAR(60),
  subprojectcode   CHAR(60),
  fundcode         CHAR(6),
  t_date           DATE"YYYY-MM-DD HH24:MI:SS" ,
  expectdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  fundacco         CHAR(12),
  businflag        CHAR(2),
  f_businbalance   ,
  contractserialno ,
  bankserialno     ,
  businserialno    ,
  fromaccoid       CHAR(6),
  fromsubaccoid    CHAR(6),
  toaccoid         CHAR(6),
  direction        CHAR(2),
  seq              ,
  remitserialno    ,
  bremitserialno   ,
  opflag           CHAR(1),
  transcount       ,
  capflag          CHAR(2),
  frombankno       CHAR(6),
  frombankname     CHAR(150),
  fromnameinbank   CHAR(300),
  frombankacco     CHAR(100),
  fromprovincecode CHAR(3),
  fromcityno       CHAR(4),
  frombanklinecode CHAR(20),
  tobankno         CHAR(6),
  tobankname       CHAR(150),
  tonameinbank     CHAR(300),
  tobankacco       CHAR(100),
  toprovincecode   CHAR(3),
  tocityno         CHAR(4),
  tobanklinecode   CHAR(20),
  remittype        CHAR(200),
  busindate        DATE"YYYY-MM-DD HH24:MI:SS" ,
  tofundcode       CHAR(6),
  remark           CHAR(1000),
  moneytype        CHAR(3),
  remitflag        CHAR(1),
  userlock         CHAR(20),
  transtype        CHAR(1),
  relateaccocode   CHAR(32),
  needremit        CHAR(1),
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
