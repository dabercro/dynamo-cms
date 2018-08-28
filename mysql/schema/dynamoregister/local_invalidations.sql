CREATE TABLE `local_invalidations` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `lfn` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
