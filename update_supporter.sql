-- death detection
UPDATE `rsm_mandate` AS `m`
JOIN `blotto_player` AS `p`
  ON `p`.`client_ref`=`m`.`ClientRef`
JOIN `blotto_supporter` AS `s`
  ON `s`.`id`=`p`.`supporter_id`
LEFT JOIN (
  SELECT
    `DDRefOrig`
   ,DATE_ADD(DATE_ADD(MAX(`DateDue`),INTERVAL 1 MONTH),INTERVAL 7 DAY) AS `DateKnown`
  FROM `rsm_collection`
  GROUP BY `DDRefOrig`
) AS `c`
  ON `c`.`DDRefOrig`=`m`.`DDRefOrig`
SET
  -- blotto code prefers notional milestone dates (that can be derived by principle) over unpredictable reality
  -- the notional discovery date for this is essentially the same as when we discover cancellation by rule
  -- that is, the last successful collection against a code 2 mandate tells us when the software discovered death
  -- so if the collection due date has been missed and we have waited 7 days to make sure BACS is not having a fit
  -- if this runs a lot we can assume discovery date is a close enough proxy for date the software "reported the death"
  `s`.`death_reported`=IFNULL(`c`.`DateKnown`,DATE_ADD(DATE_ADD(`m`.`StartDate`,INTERVAL 1 MONTH),INTERVAL 7 DAY))
WHERE `m`.`FailReason` LIKE '2:%'
  AND `s`.`death_reported` IS NULL
;
-- are there other things?

