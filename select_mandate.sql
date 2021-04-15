-- Must be a single select query
SELECT
  'RSM'
 ,null
 ,`m`.`DDRefOrig`
 ,`m`.`ClientRef`
 ,`mcreated`.`FirstCreated`
 ,`m`.`Updated`
 ,`mcreated`.`FirstStartDate`
 ,`m`.`Status`
 ,`m`.`Freq`
 ,`m`.`Amount`
 ,`m`.`ChancesCsv`
 ,`m`.`Name`
 ,`m`.`Sortcode`
 ,`m`.`Account`
 ,`m`.`FailReason`
 ,`m`.`id`
 ,`mcreated`.`TimesCreated`
 ,`m`.`Created`
 ,`m`.`StartDate`
FROM (
  SELECT
    `DDRefOrig`
   ,COUNT(`id`) AS `TimesCreated`
   ,MIN(`Created`) AS `FirstCreated`
   ,MIN(`StartDate`) AS `FirstStartDate`
   ,MAX(`Created`) AS `LastCreated`
  FROM `rsm_mandate`
  WHERE 1
  GROUP BY `DDRefOrig`
) AS `mcreated`
JOIN (
  SELECT
    `DDRefOrig`
   ,`Created`
   ,MAX(`Updated`) AS `LastUpdated`
  FROM `rsm_mandate`
  WHERE 1
  GROUP BY `DDRefOrig`,`Created`
) AS `mupdated`
  ON `mupdated`.`DDRefOrig`=`mcreated`.`DDRefOrig`
 AND `mupdated`.`Created`=`mcreated`.`LastCreated`
JOIN (
  SELECT
    `DDRefOrig`
   ,`Created`
   ,`Updated`
   ,MAX(`Status`='LIVE') AS `is_live`
  FROM `rsm_mandate`
  WHERE 1
  GROUP BY `DDRefOrig`,`Created`,`Updated`
) AS `mstatus`
  ON `mstatus`.`DDRefOrig`=`mupdated`.`DDRefOrig`
 AND `mstatus`.`Created`=`mupdated`.`Created`
 AND `mstatus`.`Updated`=`mupdated`.`LastUpdated`
JOIN `rsm_mandate` AS `m`
  ON `m`.`DDRefOrig`=`mstatus`.`DDRefOrig`
 AND `m`.`Created`=`mstatus`.`Created`
 AND `m`.`Updated`=`mstatus`.`Updated`
 AND (`m`.`Status`='LIVE')=`mstatus`.`is_live`
GROUP BY `DDRefOrig`
ORDER BY `DDRefOrig`
;
