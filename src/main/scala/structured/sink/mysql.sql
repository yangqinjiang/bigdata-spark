create TABLE `db_spark`.`tb_word_count` (
`id` int NOT NULL AUTO_INCREMENT,
`word` varchar(255) NOT NULL,
`count` int NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `word` (`word`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
REPLACE INTO ` tb_word_count` (`id`, `word`, `count`) VALUES (NULL, ?, ?);
-- 当使用 REPLACE插入数据到表时：
/*
1)、所有字段
2）、表必须有主键
3）、要求唯一索引
*/