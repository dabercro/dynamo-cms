CREATE TABLE `phedex_requests` (
  `id` int(10) NOT NULL,
  `operation_type` enum('copy','deletion') NOT NULL,
  `operation_id` int(10) NOT NULL,
  `approved` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `operation` (`operation_type`,`operation_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
