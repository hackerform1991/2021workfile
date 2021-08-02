OPTIONS (skip=1)
LOAD DATA
INFILE "/dw-data/EBS-DATA/20190412/csv/20190412_fnd_flex_value_sets_201904.csv"
truncate
INTO TABLE EBS_FND_FLEX_VALUE_SETS
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  report_id					 		,
  FLEX_VALUE_SET_ID         		,
  FLEX_VALUE_SET_NAME        		,
  LAST_UPDATE_DATE           		DATE"DD-MM-YY",
  LAST_UPDATED_BY            		,
  CREATION_DATE              		DATE"DD-MM-YY",
  CREATED_BY                 		,
  LAST_UPDATE_LOGIN          		,
  VALIDATION_TYPE            		,
  PROTECTED_FLAG             		,
  SECURITY_ENABLED_FLAG      		,
  LONGLIST_FLAG              		,
  FORMAT_TYPE                		,
  MAXIMUM_SIZE               		,
  ALPHANUMERIC_ALLOWED_FLAG  		,
  UPPERCASE_ONLY_FLAG        		,
  NUMERIC_MODE_ENABLED_FLAG  		,
  DESCRIPTION                		,
  DEPENDANT_DEFAULT_VALUE    		,
  DEPENDANT_DEFAULT_MEANING  		,
  PARENT_FLEX_VALUE_SET_ID   		,
  MINIMUM_VALUE              		,
  MAXIMUM_VALUE              		,
  NUMBER_PRECISION           		,
  ZD_EDITION_NAME            		,
  ZD_SYNC                    		,
data_date  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
