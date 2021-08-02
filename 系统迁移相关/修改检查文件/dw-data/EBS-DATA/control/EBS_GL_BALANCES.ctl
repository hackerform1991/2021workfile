OPTIONS (skip=1)
LOAD DATA
INFILE "/dw-data/EBS-DATA/20190412/csv/20190412_gl_balances_201904.csv"
truncate
INTO TABLE EBS_GL_BALANCES
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  report_id				 ,
  LEDGER_ID              ,
  CODE_COMBINATION_ID    ,
  CURRENCY_CODE          ,
  PERIOD_NAME            ,
  ACTUAL_FLAG            ,
  LAST_UPDATE_DATE       DATE"DD-MM-YY" ,
  LAST_UPDATED_BY        ,
  BUDGET_VERSION_ID      ,
  ENCUMBRANCE_TYPE_ID    ,
  TRANSLATED_FLAG        ,
  REVALUATION_STATUS     ,
  PERIOD_TYPE            ,
  PERIOD_YEAR            ,
  PERIOD_NUM             ,
  PERIOD_NET_DR          ,
  PERIOD_NET_CR          ,
  PERIOD_TO_DATE_ADB     ,
  QUARTER_TO_DATE_DR     ,
  QUARTER_TO_DATE_CR     ,
  QUARTER_TO_DATE_ADB    ,
  YEAR_TO_DATE_ADB       ,
  PROJECT_TO_DATE_DR     ,
  PROJECT_TO_DATE_CR     ,
  PROJECT_TO_DATE_ADB    ,
  BEGIN_BALANCE_DR       ,
  BEGIN_BALANCE_CR       ,
  PERIOD_NET_DR_BEQ      ,
  PERIOD_NET_CR_BEQ      ,
  BEGIN_BALANCE_DR_BEQ   ,
  BEGIN_BALANCE_CR_BEQ   ,
  TEMPLATE_ID            ,
  ENCUMBRANCE_DOC_ID     ,
  ENCUMBRANCE_LINE_NUM   ,
  QUARTER_TO_DATE_DR_BEQ ,
  QUARTER_TO_DATE_CR_BEQ ,
  PROJECT_TO_DATE_DR_BEQ ,
  PROJECT_TO_DATE_CR_BEQ ,
data_date  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
