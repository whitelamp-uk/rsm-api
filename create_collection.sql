

DROP TABLE IF EXISTS `rsm_collection`;

CREATE TABLE `rsm_collection` (
  `ClientRef` varchar(255) CHARACTER SET ascii DEFAULT NULL,
  `DDRefOrig` varchar(64) CHARACTER SET ascii DEFAULT NULL,
  `DateDue` date DEFAULT NULL,
  `Amount` decimal(10,2) DEFAULT NULL,
  `PayStatus` varchar(255) CHARACTER SET ascii DEFAULT NULL,
  `PaidAmount` decimal (10,2) DEFAULT NULL,
  KEY `DDRefOrig` (`DDRefOrig`),
  KEY `ClientRef` (`ClientRef`),
  KEY `DateDue` (`DateDue`),
  KEY `Amount` (`Amount`),
  KEY `PayStatus` (`PayStatus`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

