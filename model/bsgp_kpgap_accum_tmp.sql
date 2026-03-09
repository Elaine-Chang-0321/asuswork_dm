CREATE EXTERNAL TABLE `bsgp_kpgap_accum_tmp`(
    `product_line` string COMMENT 'from deserializer', 
    `category` string COMMENT 'from deserializer', 
    `ag_no` string COMMENT 'from deserializer', 
    `gl_yearmonth` string COMMENT 'from deserializer', 
    `equo_price` string COMMENT 'from deserializer', 
    `cogs_price` string COMMENT 'from deserializer', 
    `bs_gap` string COMMENT 'from deserializer'
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar' = ','
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://consumer-npspo/StaticFiles/armourycrate/bsgp_kpgap_accum_tmp/'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);