--将广告数据ETL后保存到Hive 分区表中，
--启动Hive交互式命令行【$HIVE_HOME/bin/hive】
--（必须在Hive中创建，否则有问题），
--创建数据库【itcast_ads】和表【pmt_ads_info】。


-- 创建 数据库 itcast_ads
-- 创建数据库，不存在时创建
-- DROP DATABASE IF EXISTS itcast_ads;
CREATE DATABASE IF NOT EXISTS itcast_ads;
USE itcast_ads;

--在Spark中不创建表，直接saveAsTable写入表且指定分区列时，
--Hive中可以查询表数据但查不到表的创建和修改信息，
--此时创建的表也不是分区表。

-- 设置表的数据snappy压缩
set parquet.compression=snappy;
-- 创建表，不存在时创建

--创建表【pmt_ads_info】，设置日期分区字段【date_str】，数据存储为parquet格式
-- DROP TABLE IF EXISTS itcast_ads.pmt_ads_info;
CREATE TABLE IF NOT EXISTS itcast_ads.pmt_ads_info(
adcreativeid BIGINT,
adorderid BIGINT,
adpayment DOUBLE,
adplatformkey STRING,
adplatformproviderid BIGINT,
adppprice DOUBLE,
adprice DOUBLE,
adspacetype BIGINT,
adspacetypename STRING,
advertisersid BIGINT,
adxrate DOUBLE,
age STRING,
agentrate DOUBLE,
ah BIGINT,
androidid STRING,
androididmd5 STRING,
androididsha1 STRING,
appid STRING,
appname STRING,
apptype BIGINT,
aw BIGINT,
bidfloor DOUBLE,
bidprice DOUBLE,
callbackdate STRING,
channelid STRING,
cityname STRING,
client BIGINT,
cnywinprice DOUBLE,
cur STRING,
density STRING,
device STRING,
devicetype BIGINT,
district STRING,
email STRING,
idfa STRING,
idfamd5 STRING,
idfasha1 STRING,
imei STRING,
imeimd5 STRING,
imeisha1 STRING,
initbidprice DOUBLE,
ip STRING,
iptype BIGINT,
isbid BIGINT,
isbilling BIGINT,
iseffective BIGINT,
ispid BIGINT, ispname STRING,
isqualityapp BIGINT,
iswin BIGINT,
keywords STRING,
lang STRING,
lat STRING,
lomarkrate DOUBLE,
mac STRING,
macmd5 STRING,
macsha1 STRING,
mediatype BIGINT,
networkmannerid BIGINT,
networkmannername STRING,
openudid STRING,
openudidmd5 STRING,
openudidsha1 STRING,
osversion STRING,
paymode BIGINT,
ph BIGINT,
processnode BIGINT,
provincename STRING,
putinmodeltype BIGINT,
pw BIGINT,
rate DOUBLE,
realip STRING,
reqdate STRING,
reqhour STRING,
requestdate STRING,
requestmode BIGINT,
rtbcity STRING,
rtbdistrict STRING,
rtbprovince STRING,
rtbstreet STRING,
sdkversion STRING,
sessionid STRING,
sex STRING,
storeurl STRING,
tagid STRING,
tel STRING,
title STRING,
userid STRING,
uuid STRING,
uuidunknow STRING,
winprice DOUBLE,
province STRING,
city STRING
)
PARTITIONED BY (date_str string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';
-- STORED AS PARQUET ;

-- 待DataFrame将数据保存Hive表
SHOW CREATE TABLE itcast_ads.pmt_ads_info ;
SHOW PARTITIONS itcast_ads.pmt_ads_info ;
SELECT ip, province, city FROM itcast_ads.pmt_ads_info WHERE date_str = '' limit 10;
-- 删除分区表数据
ALTER TABLE itcast_ads.pmt_ads_info DROP IF EXISTS PARTITION (date_str='2020-04-23');