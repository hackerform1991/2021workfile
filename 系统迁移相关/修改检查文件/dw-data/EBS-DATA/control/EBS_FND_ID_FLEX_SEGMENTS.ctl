OPTIONS (skip=1)
LOAD DATA
INFILE "/dw-data/EBS-DATA/20190412/csv/20190412_fnd_id_flex_segments_201904.csv"
truncate
INTO TABLE EBS_FND_ID_FLEX_SEGMENTS
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
report_id,
   APPLICATION_ID                ,
  ID_FLEX_CODE                  ,
  ID_FLEX_NUM                   ,
  APPLICATION_COLUMN_NAME       ,
  SEGMENT_NAME                  ,
  LAST_UPDATE_DATE              DATE"DD-MM-YY",
  LAST_UPDATED_BY               ,
  CREATION_DATE                 DATE"DD-MM-YY",
  CREATED_BY                    ,
  LAST_UPDATE_LOGIN             ,
  SEGMENT_NUM                   ,
  APPLICATION_COLUMN_INDEX_FLAG ,
  ENABLED_FLAG                  ,
  REQUIRED_FLAG                 ,
  DISPLAY_FLAG                  ,
  DISPLAY_SIZE                  ,
  SECURITY_ENABLED_FLAG         ,
  MAXIMUM_DESCRIPTION_LEN       ,
  CONCATENATION_DESCRIPTION_LEN ,
  FLEX_VALUE_SET_ID             ,
  RANGE_CODE                    ,
  DEFAULT_TYPE                  ,
  DEFAULT_VALUE                 ,
  RUNTIME_PROPERTY_FUNCTION     ,
  ADDITIONAL_WHERE_CLAUSE       ,
  ZD_EDITION_NAME               ,
  ZD_SYNC                       ,
data_date  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
