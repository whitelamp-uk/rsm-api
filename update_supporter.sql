-- death detection
UPDATE `rsm_mandate` AS `m`
JOIN `blotto_player` AS `p`
  ON `p`.`client_ref`=`m`.`ClientRef`
JOIN `blotto_supporter` AS `s`
  ON `s`.`id`=`p`.`supporter_id`
LEFT JOIN (
  SELECT
    `DDRefOrig`
   ,DATE_ADD(DATE_ADD(`DateDue`,INTERVAL 1 MONTH),INTERVAL 7 DAY) AS `DateKnown`
  FROM `rsm_collection`
  GROUP BY `DDRefOrig`
) AS `c`
  ON `c`.`DDRefOrig`=`m`.`DDRefOrig`
SET
  `s`.`death_reported`=IFNULL(`c`.`DateKnown`,DATE_ADD(DATE_ADD(`m`.`StartDate`,INTERVAL 1 MONTH),INTERVAL 7 DAY))
WHERE `m`.`FailReason` LIKE '2:%'
  AND `s`.`death_reported` IS NULL
;
-- are there other things?

