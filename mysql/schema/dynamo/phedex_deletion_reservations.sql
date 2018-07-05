CREATE TABLE `phedex_deletion_reservations` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  KEY `reservation` (`item`,`site`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
