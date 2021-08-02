OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-PaymentReceipt.csv"
truncate
INTO TABLE AICS_PAYMENT_RECEIPT
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  br                 CHAR(32),
  dealno             CHAR(32),
  seq                CHAR(3),
  projectcode        CHAR(32),
  subproject         CHAR(32),
  module             CHAR(32),
  type               CHAR(32),
  type2              CHAR(32),
  busitype           CHAR(8),
  paydate            DATE"YYYY-MM-DD HH24:MI:SS" ,
  payrecind          CHAR(1),
  ccy                CHAR(3),
  amount             ,
  custno             CHAR(32),
  t_desc             CHAR(300),
  remark             CHAR(300),
  status             CHAR(1),
  pbnknm             CHAR(300),
  pacctno            CHAR(300),
  pacctnm            CHAR(300),
  pcnapscode         CHAR(12),
  rbnknm             CHAR(300),
  racctno            CHAR(300),
  racctnm            CHAR(300),
  rcnapscode         CHAR(12),
  process_instanceid CHAR(32),
  createuser         CHAR(32),
  createtime         DATE"YYYY-MM-DD HH24:MI:SS" ,
  verifyuser         CHAR(32),
  verifytime         DATE"YYYY-MM-DD HH24:MI:SS" ,
  reversaluser       CHAR(32),
  reversaltime       DATE"YYYY-MM-DD HH24:MI:SS" ,
  psettlement_path   CHAR(32),
  rsettlement_path   CHAR(32),
  paccttype          CHAR(32),
  raccttype          CHAR(32),
  auto_settlement    CHAR(1),
  enter_account      CHAR(1),
datadate  "(select data_date from etl_control.ctl_general_parameter)"
)
