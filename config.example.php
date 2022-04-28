<?php

define ( 'BLOTTO_PAY_API_RSM',          '/some/rsm-api/PayApi.php'    );
define ( 'BLOTTO_PAY_API_RSM_CLASS',    '\Blotto\Rsm\PayApi'          );
define ( 'BLOTTO_PAY_API_RSM_SELECT',   'SELECT DISTINCT(`ClientRef`) AS `crf` FROM `rsm_mandate`' );

define ( 'RSM_URL',                     'https://rsm5.rsmsecure.com/ddcm/ddcmApi.php' );
define ( 'RSM_USER',                    'my_rsm_api'                  );
define ( 'RSM_PASSWORD',                '**********'                  );
define ( 'RSM_ERROR_LOG',               false                         );
define ( 'RSM_FILE_DEBOGON',            '/my/debogon.sql'             );
define ( 'RSM_PAY_INTERVAL',            '2 DAY' ); // Ignore recent collections - see BACS behaviour
define ( 'RSM_TABLE_MANDATE',           'your_mandate_table'          );
define ( 'RSM_TABLE_COLLECTION',        'your_collection_table'       );


