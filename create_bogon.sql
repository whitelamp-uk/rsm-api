
DELIMITER $$
DROP PROCEDURE IF EXISTS `bogonCheckAmounts`$$
CREATE PROCEDURE `bogonCheckAmounts` (
)
BEGIN
  INSERT INTO `rsm_bogon`
    SELECT
      null
     ,'Mandate Amount not consistent with collection PaidAmount'
     ,CONCAT(
        '[ '
       ,`c`.`ClientRef`
       ,' x'
       ,COUNT(`c`.`id`)
       ,' collections @'
       ,`c`.`PaidAmount`
       ,' ] conflicts with [ '
       ,`m`.`ClientRef`
       ,' mandate @'
       ,`m`.`Amount`
       ,' ]'
      )
     ,null
    FROM `rsm_collection` AS `c`
    JOIN `rsm_mandate` AS `m`
      ON `m`.`DDRefOrig`=`c`.`DDRefOrig`
    WHERE `c`.`PaidAmount`>0
      AND `m`.`Amount`!=`c`.`PaidAmount`
      AND `m`.`Amount`/5!=`c`.`PaidAmount`/4.34
      AND `m`.`Amount`/4.34!=`c`.`PaidAmount`/5
      AND `m`.`Amount`/60!=`c`.`PaidAmount`/52
      AND `m`.`Amount`/52!=`c`.`PaidAmount`/60
    GROUP BY `c`.`DDRefOrig`
  ;
END$$


DELIMITER $$
DROP PROCEDURE IF EXISTS `bogonCheckClientRef`$$
CREATE PROCEDURE `bogonCheckClientRef` (
)
BEGIN
  INSERT INTO `rsm_bogon`
  SELECT
    null
   ,'Mandate ClientRef:DDRefOrig not 1:1'
   ,CONCAT(
      `m1`.`DDRefOrig`
     ,'[ #'
     ,`m2`.`id`
     ,' '
     ,`m2`.`ClientRef`
     ,' created='
     ,`m2`.`Created`
     ,' start='
     ,`m2`.`StartDate`
     ,' '
     ,`m2`.`Status`
     ,' ] conflicts with [ #'
     ,`m1`.`id`
     ,' '
     ,`m1`.`ClientRef`
     ,' created='
     ,`m1`.`Created`
     ,' start='
     ,`m1`.`StartDate`
     ,' '
     ,`m1`.`Status`
     ,' ]'
    )
   ,null
  FROM `rsm_mandate` AS `m1`
  JOIN `rsm_mandate` AS `m2`
    ON (
         `m2`.`DDRefOrig`=`m1`.`DDRefOrig`
     AND `m2`.`ClientRef`!=`m1`.`ClientRef`
    ) 
    OR (
         `m2`.`ClientRef`=`m1`.`ClientRef`
     AND `m2`.`DDRefOrig`!=`m1`.`DDRefOrig`
    ) 
   AND (
       `m2`.`Created`>=`m1`.`Created`
    OR `m2`.`Created` IS NULL
   )
  ;
END$$


DELIMITER $$
DROP PROCEDURE IF EXISTS `bogonCheckCollections`$$
CREATE PROCEDURE `bogonCheckCollections` (
)
BEGIN
  INSERT INTO `rsm_bogon`
    SELECT
      null
     ,'Collection/mandate DDRefOrig/Clientref inconsistent'
     ,IF (
        `m`.`id` IS NULL
       ,CONCAT(
          '[ '
         ,`c`.`ClientRef`
         ,' x'
         ,COUNT(`c`.`id`)
         ,' collections ] have no corresponding mandate'
        )
       ,CONCAT(
          '[ #'
         ,' '
         ,`c`.`ClientRef`
         ,' x'
         ,COUNT(`c`.`id`)
         ,' collections ] conflicts with [ #'
         ,IF(
            `m`.`id` IS NULL
           ,''
           ,CONCAT(
              `m`.`id`
             ,' '
             ,`m`.`ClientRef`
             ,' '
             ,`m`.`DDRefOrig`
             ,' created='
             ,`m`.`Created`
             ,' start='
             ,`m`.`StartDate`
             ,' '
             ,`m`.`Status`
            )
          )
         ,' ]'
        )
      )
     ,null
    FROM `rsm_collection` AS `c`
    LEFT JOIN `rsm_mandate` AS `m`
      ON `m`.`ClientRef`=`c`.`ClientRef`
    WHERE `c`.`PaidAmount`>0
      AND `m`.`id` IS NULL
       OR `m`.`DDRefOrig`!=`c`.`DDRefOrig`
    GROUP BY `m`.`id`
  ;
END$$


DELIMITER $$
DROP PROCEDURE IF EXISTS `bogonCheckDateDue`$$
CREATE PROCEDURE `bogonCheckDateDue` (
)
BEGIN
  INSERT INTO `rsm_bogon`
    SELECT
      null
     ,'Collection DateDue not unique per DDRefOrig'
     ,CONCAT_WS(
        ', '
       ,`DDRefOrig`
       ,`ClientRef`
       ,`DateDue`
       ,CONCAT(
          COUNT(`id`)
         ,' payments on the same date'
        )
      )
     ,COUNT(`id`) AS `qty`
    FROM `rsm_collection`
    WHERE `PaidAmount`>0
    GROUP BY `ClientRef`,`DateDue`
    HAVING `qty`>1
  ;
END$$


DELIMITER $$
DROP PROCEDURE IF EXISTS `bogonCheckFreqAmount`$$
CREATE PROCEDURE `bogonCheckFreqAmount` (
)
BEGIN
  INSERT INTO `rsm_bogon`
    SELECT
      null
     ,'Mandate Freq-Amount not unique per DDRefOrig'
     ,CONCAT(
        `m1`.`DDRefOrig`
       ,'[ #'
       ,`m2`.`id`
       ,' created='
       ,`m2`.`Created`
       ,' start='
       ,`m2`.`StartDate`
       ,' '
       ,`m2`.`Status`
       ,' ] conflicts with [ #'
       ,`m1`.`id`
       ,' created='
       ,`m1`.`Created`
       ,' start='
       ,`m1`.`StartDate`
       ,' '
       ,`m1`.`Status`
       ,' ]'
      )
     ,null
    FROM `rsm_mandate` AS `m1`
    JOIN `rsm_mandate` AS `m2`
      ON `m2`.`DDRefOrig`=`m1`.`DDRefOrig`
     AND (
         `m2`.`Freq`!=`m1`.`Freq`
         OR (
           `m2`.`Amount`!=`m1`.`Amount`
            AND `m2`.`Amount`/5!=`m1`.`Amount`/4.34
            AND `m2`.`Amount`/4.34!=`m1`.`Amount`/5
            AND `m2`.`Amount`/60!=`m1`.`Amount`/52
            AND `m2`.`Amount`/52!=`m1`.`Amount`/60
         )
     )
     AND `m2`.`id`!=`m1`.`id`
     AND (
         `m2`.`Created`>=`m1`.`Created`
      OR `m2`.`Created` IS NULL
     )
  ;
END$$


DELIMITER $$
DROP PROCEDURE IF EXISTS `bogonCheckLive`$$
CREATE PROCEDURE `bogonCheckLive` (
)
BEGIN
  INSERT INTO `rsm_bogon`
    SELECT
      null
     ,'Mandate Status LIVE not unique per DDRefOrig'
     ,CONCAT(
        `m1`.`DDRefOrig`
       ,'[ #'
       ,`m2`.`id`
       ,' created='
       ,`m2`.`Created`
       ,' start='
       ,`m2`.`StartDate`
       ,' '
       ,`m2`.`Status`
       ,' ] conflicts with [ #'
       ,`m1`.`id`
       ,' created='
       ,`m1`.`Created`
       ,' start='
       ,`m1`.`StartDate`
       ,' '
       ,`m1`.`Status`
       ,' ]'
      )
     ,null
    FROM `rsm_mandate` AS `m1`
    JOIN `rsm_mandate` AS `m2`
      ON `m2`.`DDRefOrig`=`m1`.`DDRefOrig`
     AND `m2`.`id`>`m1`.`id`
    WHERE `m1`.`Status`='LIVE'
      AND `m2`.`Status`='LIVE'
  ;
END$$


DELIMITER $$
DROP PROCEDURE IF EXISTS `bogonCheckPaidAmount`$$
CREATE PROCEDURE `bogonCheckPaidAmount` (
)
BEGIN
  INSERT INTO `rsm_bogon`
    SELECT
      null
     ,'Collection PaidAmount not unique per DDRefOrig'
     ,CONCAT_WS(
        ', '
       ,`DDRefOrig`
       ,`ClientRef`
       ,CONCAT(
          COUNT(DISTINCT `PaidAmount`)
         ,' different collection amounts: '
         ,GROUP_CONCAT(DISTINCT `PaidAmount`)
        )
      )
     ,COUNT(DISTINCT `PaidAmount`) AS `qty`
    FROM `rsm_collection`
    WHERE `PaidAmount`>0
    GROUP BY `ClientRef`
    HAVING `qty`>1
  ;
END$$


DROP TABLE IF EXISTS `rsm_bogon`;

CREATE TABLE `rsm_bogon` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `type` varchar(64) CHARACTER SET ascii DEFAULT NULL,
  `details` varchar(255) CHARACTER SET ascii DEFAULT NULL,
  `tmp` int(11) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CALL `bogonCheckDateDue`();
CALL `bogonCheckPaidAmount`();
CALL `bogonCheckClientRef`();
CALL `bogonCheckCollections`();
CALL `bogonCheckFreqAmount`();
CALL `bogonCheckAmounts`();
CALL `bogonCheckLive`();

ALTER TABLE `rsm_bogon`
DROP COLUMN `tmp`
;

DROP PROCEDURE `bogonCheckDateDue`;
DROP PROCEDURE `bogonCheckPaidAmount`;
DROP PROCEDURE `bogonCheckClientRef`;
DROP PROCEDURE `bogonCheckCollections`;
DROP PROCEDURE `bogonCheckFreqAmount`;
DROP PROCEDURE `bogonCheckAmounts`;
DROP PROCEDURE `bogonCheckLive`;

