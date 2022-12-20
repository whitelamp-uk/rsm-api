-- Must be a single select query
SELECT
  'RSM'
 ,null
 ,`m`.`DDRefOrig`
 ,`m`.`ClientRef`
 ,`aggregate`.`FirstCreated`
 ,`aggregate`.`LastUpdated`
 ,`aggregate`.`FirstStartDate`
 ,`m`.`Status`
 ,`m`.`Freq`
 ,`m`.`Amount`
 ,`m`.`ChancesCsv`
 ,`m`.`Name`
 ,`m`.`Sortcode`
 ,`m`.`Account`
 ,`m`.`FailReason`
 ,`m`.`id`
 ,`aggregate`.`TimesCreated`
 ,`m`.`Created`
 ,`m`.`StartDate`
FROM (
  SELECT
    `DDRefOrig`
   ,COUNT(`id`) AS `TimesCreated`
   ,MIN(`Created`) AS `FirstCreated`
   ,MIN(`StartDate`) AS `FirstStartDate`
   ,MAX(`Created`) AS `LastCreated`
   ,MAX(`Updated`) AS `LastUpdated`
  FROM `rsm_mandate`
  WHERE 1
  GROUP BY `DDRefOrig`
) AS `aggregate`
JOIN `rsm_mandate` AS `m`
  ON `m`.`DDRefOrig`=`aggregate`.`DDRefOrig`
 AND `m`.`IsCurrent`>0
{{WHERE}}
GROUP BY `DDRefOrig`
ORDER BY `DDRefOrig`
;
