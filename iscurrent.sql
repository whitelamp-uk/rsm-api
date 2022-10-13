
DELIMITER $$
DROP PROCEDURE IF EXISTS `bogonCheckIsCurrent`$$
CREATE PROCEDURE `bogonCheckIsCurrent` (
)
BEGIN
  -- This routine is not in create_bogon.sql because it checks results
  -- from setIsCurrent() rather than checking RSM source data
  INSERT INTO `rsm_bogon`
  SELECT
    'setIsCurrent() is misbehaving'
   ,CONCAT (`quantity_ddrefs`,' mandates with ',`quantity_current`,' current rows (where IsCurrent=1): ',`ddrefs`)
   ,null
  FROM (
    SELECT
      `t`.`qty` AS `quantity_current`
     ,COUNT(`t`.`DDRefOrig`) AS `quantity_ddrefs`
   ,GROUP_CONCAT(`t`.`DDRefOrig` SEPARATOR ',') AS `ddrefs`
    FROM (
      SELECT
        `DDRefOrig`
       ,SUM(`IsCurrent`>0) as qty
      FROM `rsm_mandate`
      WHERE `Status`!='PENDING'
      GROUP BY `DDRefOrig`
    ) AS `t`
    GROUP BY `quantity_current`
    HAVING `quantity_current`!=1
  ) AS `b`
  ;
END$$



DELIMITER $$
DROP PROCEDURE IF EXISTS `setIsCurrent`$$
CREATE PROCEDURE `setIsCurrent` (
)
BEGIN

  UPDATE `rsm_mandate`
  SET `IsCurrent`=0
  ;
  UPDATE `rsm_mandate`
  SET `IsCurrent`=1
  WHERE `Status`='LIVE'
  ;

  -- Get the rows matching the max start date
  DROP TABLE IF EXISTS `rsm_tmp_1`
  ;
  CREATE TABLE `rsm_tmp_1` AS
    SELECT
      `m`.*
    FROM (
      SELECT
        `DDRefOrig`
       ,MAX(`StartDate`) AS `LastStartDate`
       ,MAX(`IsCurrent`) AS `Current`
      FROM `rsm_mandate`
      WHERE `Status`!='PENDING'
      GROUP BY `DDRefOrig`
      HAVING `Current`=0
    ) AS `started`
    JOIN `rsm_mandate` AS `m`
      ON `m`.`DDRefOrig`=`started`.`DDRefOrig`
     AND `m`.`StartDate`=`started`.`LastStartDate`
    ORDER BY `m`.`DDRefOrig`,`started`.`LastStartDate`
  ;

  -- Count the rows with the same start date
  DROP TABLE IF EXISTS `rsm_tmp_2`
  ;
  CREATE TABLE `rsm_tmp_2` AS
    SELECT
      GROUP_CONCAT(`id` SEPARATOR ',') AS `ids`
     ,`DDRefOrig`
     ,COUNT(`id`) AS `qty`
    FROM `rsm_tmp_1`
    GROUP BY `DDRefOrig`
  ;

  -- Set IsCurrent for single max start date
  UPDATE `rsm_mandate` AS `m`
  JOIN `rsm_tmp_2` AS `started`
    ON `started`.`ids`=`m`.`id`
  SET
    `IsCurrent`=1
  WHERE `started`.`qty`=1
  ;

  -- Get the rows matching the max created date
  DROP TABLE IF EXISTS `rsm_tmp_1`
  ;
  CREATE TABLE `rsm_tmp_1` AS
    SELECT
      `m`.*
    FROM (
      SELECT
        `DDRefOrig`
       ,MAX(`Created`) AS `LastCreated`
       ,MAX(`IsCurrent`) AS `Current`
      FROM `rsm_mandate`
      WHERE `Status`!='PENDING'
      GROUP BY `DDRefOrig`
      HAVING `Current`=0
    ) AS `created`
    JOIN `rsm_mandate` AS `m`
      ON `m`.`DDRefOrig`=`created`.`DDRefOrig`
     AND `m`.`Created`=`created`.`LastCreated`
    ORDER BY `m`.`DDRefOrig`,`created`.`LastCreated`
  ;

  -- Count the rows with the same created date
  DROP TABLE IF EXISTS `rsm_tmp_2`
  ;
  CREATE TABLE `rsm_tmp_2` AS
    SELECT
      GROUP_CONCAT(`id` SEPARATOR ',') AS `ids`
     ,`DDRefOrig`
     ,COUNT(`id`) AS `qty`
    FROM `rsm_tmp_1`
    GROUP BY `DDRefOrig`
  ;

  -- Set IsCurrent for single max created date
  UPDATE `rsm_mandate` AS `m`
  JOIN `rsm_tmp_2` AS `created`
    ON `created`.`ids`=`m`.`id`
  SET
    `IsCurrent`=1
  WHERE `created`.`qty`=1
  ;


  -- Get the rows with a fail reason
  DROP TABLE IF EXISTS `rsm_tmp_1`
  ;
  CREATE TABLE `rsm_tmp_1` AS
    SELECT
      `m`.*
    FROM (
      SELECT
        `DDRefOrig`
       ,MAX(`IsCurrent`) AS `Current`
      FROM `rsm_mandate`
      WHERE `Status`!='PENDING'
      GROUP BY `DDRefOrig`
      HAVING `Current`=0
    ) AS `failed`
    JOIN `rsm_mandate` AS `m`
      ON `m`.`DDRefOrig`=`failed`.`DDRefOrig`
     AND `m`.`FailReason`!=''
    ORDER BY `m`.`DDRefOrig`
  ;

  -- Count the rows with a fail reason
  DROP TABLE IF EXISTS `rsm_tmp_2`
  ;
  CREATE TABLE `rsm_tmp_2` AS
    SELECT
      GROUP_CONCAT(`id` SEPARATOR ',') AS `ids`
     ,`DDRefOrig`
     ,COUNT(`id`) AS `qty`
    FROM `rsm_tmp_1`
    GROUP BY `DDRefOrig`
  ;

  -- Set IsCurrent for single fail reason
  UPDATE `rsm_mandate` AS `m`
  JOIN `rsm_tmp_2` AS `failed`
    ON `failed`.`ids`=`m`.`id`
  SET
    `IsCurrent`=1
  WHERE `failed`.`qty`=1
  ;


  -- Get the rows matching the max updated date
  DROP TABLE IF EXISTS `rsm_tmp_1`
  ;
  CREATE TABLE `rsm_tmp_1` AS
    SELECT
      `m`.*
    FROM (
      SELECT
        `DDRefOrig`
       ,MAX(`Updated`) AS `LastUpdated`
       ,MAX(`IsCurrent`) AS `Current`
      FROM `rsm_mandate`
      WHERE `Status`!='PENDING'
      GROUP BY `DDRefOrig`
      HAVING `Current`=0
    ) AS `updated`
    JOIN `rsm_mandate` AS `m`
      ON `m`.`DDRefOrig`=`updated`.`DDRefOrig`
     AND `m`.`Updated`=`updated`.`LastUpdated`
    ORDER BY `m`.`DDRefOrig`,`updated`.`LastUpdated`
  ;

  -- Count the rows with the same updated date
  DROP TABLE IF EXISTS `rsm_tmp_2`
  ;
  CREATE TABLE `rsm_tmp_2` AS
    SELECT
      GROUP_CONCAT(`id` SEPARATOR ',') AS `ids`
     ,`DDRefOrig`
     ,COUNT(`id`) AS `qty`
    FROM `rsm_tmp_1`
    GROUP BY `DDRefOrig`
  ;

  -- Set IsCurrent for single max updated date
  UPDATE `rsm_mandate` AS `m`
  JOIN `rsm_tmp_2` AS `updated`
    ON `updated`.`ids`=`m`.`id`
  SET
    `IsCurrent`=1
  WHERE `updated`.`qty`=1
  ;

  -- Tidy up
--  DROP TABLE IF EXISTS `rsm_tmp_1`
--  ;
--  DROP TABLE IF EXISTS `rsm_tmp_2`
--  ;

END$$



CALL `setIsCurrent`();
CALL `bogonCheckIsCurrent`();


-- DROP PROCEDURE `setIsCurrent`;
-- DROP PROCEDURE `bogonCheckIsCurrent`;

