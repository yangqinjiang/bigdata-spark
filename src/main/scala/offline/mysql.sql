
-- 创建数据库，不存在时创建
-- DROP DATABASE IF EXISTS itcast_ads_report;
CREATE DATABASE IF NOT EXISTS itcast_ads_report;


USE itcast_ads_report;
-- 创建表
-- DROP TABLE IF EXISTS itcast_ads_report.region_stat_analysis ;
CREATE TABLE `itcast_ads_report`.`region_stat_analysis` (
`report_date` varchar(255) NOT NULL,
`province` varchar(255) NOT NULL,
`city` varchar(255) NOT NULL,
`count` bigint DEFAULT NULL,
PRIMARY KEY (`report_date`,`province`,`city`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





-- 创建表
-- DROP TABLE IF EXISTS itcast_ads_report.ads_region_analysis ;
CREATE TABLE `itcast_ads_report`.`ads_region_analysis` (
`report_date` varchar(255) NOT NULL,
`province` varchar(255) NOT NULL,
`city` varchar(255) NOT NULL,
`orginal_req_cnt` bigint DEFAULT NULL,
`valid_req_cnt` bigint DEFAULT NULL,
`ad_req_cnt` bigint DEFAULT NULL,
`join_rtx_cnt` bigint DEFAULT NULL,
`success_rtx_cnt` bigint DEFAULT NULL,
`ad_show_cnt` bigint DEFAULT NULL,
`ad_click_cnt` bigint DEFAULT NULL,
`media_show_cnt` bigint DEFAULT NULL,
`media_click_cnt` bigint DEFAULT NULL,
`dsp_pay_money` bigint DEFAULT NULL,
`dsp_cost_money` bigint DEFAULT NULL,
`success_rtx_rate` double DEFAULT NULL,
`ad_click_rate` double DEFAULT NULL,
`media_click_rate` double DEFAULT NULL,
PRIMARY KEY (`report_date`,`province`,`city`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 清空数据
 truncate table `itcast_ads_report`.`ads_region_analysis`;
