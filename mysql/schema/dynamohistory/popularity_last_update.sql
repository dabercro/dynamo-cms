CREATE TABLE `popularity_last_update` (
  `dataset_accesses_last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dataset_requests_last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00'
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
