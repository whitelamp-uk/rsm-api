

DROP TABLE IF EXISTS `rsm_mandate`;

CREATE TABLE `rsm_mandate` (
  `DDRefOrig` varchar(64) CHARACTER SET ascii DEFAULT NULL,
  `Name` varchar(255) DEFAULT NULL,
  `Sortcode` varchar(255) DEFAULT NULL,
  `Account` varchar(255) DEFAULT NULL,
  `Amount` decimal(10,2) DEFAULT NULL,
  `StartDate` varchar(255) DEFAULT NULL,
  `Freq` varchar(255) DEFAULT NULL,
  `Created` date DEFAULT NULL,
  `ClientRef` varchar(255) CHARACTER SET ascii DEFAULT NULL,
  `Status` varchar(255) CHARACTER SET ascii DEFAULT NULL,
  `FailReason` varchar(255) DEFAULT NULL,
  `Updated` varchar(255) CHARACTER SET ascii DEFAULT NULL,
  `ChancesCsv` varchar(255) CHARACTER SET ascii NOT NULL,
  KEY `DDRefOrig` (`DDRefOrig`),
  KEY `ClientRef` (`ClientRef`),
  KEY `Freq` (`Freq`),
  KEY `Amount` (`Amount`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

