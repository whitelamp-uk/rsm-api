<?php

namespace Blotto\Rsm;

class PayApi {

    private  $bogon_file;
    private  $connection;
    public   $constants = [
                 'RSM_ERROR_LOG',
                 'RSM_URL',
                 'RSM_USER',
                 'RSM_PASSWORD',
                 'RSM_PAY_INTERVAL',
                 'RSM_FILE_DEBOGON',
                 'RSM_TABLE_COLLECTION',
                 'RSM_TABLE_MANDATE'
             ];
    public   $database;
    public   $diagnostic;
    public   $error;
    public   $errorCode = 0;
    public   $frequency = [
        // FLC --> RSM
        '1'               => 'Monthly',
        'M'               => 'Monthly',
        'Monthly'         => 'Monthly',
        'OneMonthly'      => 'Monthly',
        '3'               => 'Quarterly',
        'Q'               => 'Quarterly',
        'Quarterly'       => 'Quarterly',
        'ThreeMonthly'    => 'Quarterly',
        '6'               => '6 Monthly',
        'S'               => '6 Monthly',
        '6 Monthly'       => '6 Monthly',
        'SixMonthly'      => '6 Monthly',
        '12'              => 'Annually',
        'Y'               => 'Annually',
        'Annually'        => 'Annually',
        'TwelveMonthly'   => 'Annually'
    ];


    // Translate from API mandate fields to table fields
    private $fieldsm = array (
        'ddRefNo'             => 'DDRefOrig',
        'name'                => 'Name',
        'sortcode'            => 'Sortcode',
        'account'             => 'Account',
        'amount'              => 'Amount',
        'startDate'           => 'StartDate',
        'frequency'           => 'Freq',
        'created'             => 'Created',
        'clientRef'           => 'ClientRef',
        'status'              => 'Status',
        'failReason'          => 'FailReason',
        'updated'             => 'Updated',
        'paymentReference'    => 'ChancesCsv',
    );
    // Translate from API collection fields to table fields
    private $fieldsc = array (
        'clientRef'           => 'ClientRef',
        'ddRefNo'             => 'DDRefOrig',
        'dateDue'             => 'DateDue',
        'amount'              => 'Amount',
        'payStatus'           => 'PayStatus',
        'paidAmount'          => 'PaidAmount',
    );
    private $org;


    public function __construct ($connection,$org=null) {
        $this->connection   = $connection;
        $this->org          = $org;
        $this->setup ();
    }

    public function __destruct ( ) {
    }

    public function bad_mandates ( ) {
        $bads               = [];
        $sql = "
          SELECT
            `m`.`ClientRef`
           ,`m`.`Name`
          FROM `rsm_mandate` AS `m`
          JOIN `blotto_player` AS `p`
            ON `p`.`client_ref`=`m`.`ClientRef`
          JOIN `blotto_supporter` AS `s`
            ON `s`.`id`=`p`.`supporter_id`
          -- This
          WHERE `s`.`mandate_blocked`>0
          -- conflicts with this
            AND `m`.`Status`='LIVE'
          ;
        ";
        echo "$sql\n";
        try {
            $results = $this->connection->query ($sql);
            while ($b=$results->fetch_assoc()) {
                $bads[] = $b;
            }
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (110,'SQL select failed: '.$e->getMessage());
            throw new \Exception ('SQL error');
            return false;
        }
        return $bads;
    }

    private function bogon_check ( ) {
        $this->execute (__DIR__.'/create_bogon.sql');
        $sql                = "SELECT COUNT(*) AS `qty` FROM `rsm_bogon`";
        try {
            $qty            = $this->connection->query ($sql);
            $qty            = $qty->fetch_assoc()['qty'];
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (127,'SQL select failed: '.$e->getMessage());
            throw new \Exception ('SQL database error');
            return false;
        }
        if ($qty>0) {
            $this->error_log (126,"Failed bogon check ($qty found)");
            throw new \Exception ("$qty bogons in `rsm_bogon` - ".RSM_FILE_DEBOGON." needs more work");
            return false;
        }
    }

    public function cancel_mandate ($cref) {
        $cref = $this->connection->real_escape_string($cref);

        $sql                = "SELECT `Name`,`Sortcode`,`Account`,`Amount`,`StartDate`,`Freq` FROM `rsm_mandate` ";
        $sql               .= "WHERE `ClientRef` = '$cref'";
        try {
            $res            = $this->connection->query ($sql);
            $res            = $res->fetch_assoc();
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (127,'SQL select failed: '.$e->getMessage());
            throw new \Exception ('SQL database error');
            return false;
        }

        $dt_csd = \DateTime::createFromFormat ('Y-m-d',$res['StartDate']); // is there a better way of converting?
        $rsm_startdate = $dt_csd->format ('d/m/Y');

        $what = 'setMandates';
        $body = "<mandates>";
        $body .= "<mandate>";
        $body .= "<action>D</action>";
        $body .= "<clientRef>{$cref}</clientRef>";
        $body .= "<contactName>{$res['Name']}</contactName>"; // required? - see insert_mandates
        $body .= "<accountName>{$res['Name']}</accountName>";
        $body .= "<accountNumber>{$res['Account']}</accountNumber>";
        $body .= "<sortCode>{$res['Sortcode']}</sortCode>";

        $body .= "<amount>{$res['Amount']}</amount>";
        $body .= "<frequency>{$this->frequency[$res['Freq']]}</frequency>"; // really?
        $body .= "<startDate>".$rsm_startdate."</startDate>";
            //$body .= "<paymentRef>{$m['Chances']}</paymentRef>";

        $body .= "</mandate>";
        $body .= "</mandates>";
        $request = $this->request_start ($what).$body.$this->request_end();
        error_log($request); // send to logfile

        $response = $this->handle ($what, $request);
        error_log(print_r($response,true));

        return $response;
    }

    private function curl_post ($url,$post,$options=[]) {
    /*
        * Send a POST requst using cURL
        * @param string $url to request
        * @param array $post values to send
        * @param array $options for cURL
        * @return string
    */
        if (!is_array($post) || !is_array($options)) {
            throw new \Exception ('Post and option arguments must be arrays');
            return false;
        }
        $defaults = array (
            CURLOPT_POST => 1,
            CURLOPT_HEADER => 0,
            CURLOPT_URL => $url,
            CURLOPT_FRESH_CONNECT => 1,
            CURLOPT_RETURNTRANSFER => 1,
            CURLOPT_FORBID_REUSE => 1,
            CURLOPT_TIMEOUT => 45,
            CURLOPT_POSTFIELDS => http_build_query ($post)
        );

        $ch = curl_init ();
        curl_setopt_array ($ch,$options+$defaults);
        if (!($result=curl_exec($ch))) {
            $this->error_log (125,curl_error($ch));
            throw new \Exception ("cURL POST error");
            return false;
        }
        curl_close ($ch);
        return $result;
    }

    private function debogon ( ) {
        $this->execute (RSM_FILE_DEBOGON);
    }

    public function do_heartbeat ( ) {
        return $this->handle ('heartbeat', $this->heartbeat_request ());
    }

    private function error_log ($code,$message) {
        $this->errorCode    = $code;
        $this->error        = $message;
        if (!defined('RSM_ERROR_LOG') || !RSM_ERROR_LOG) {
            return;
        }
        error_log ($code.' '.$message);
    }

    private function execute ($sql_file) {
        echo file_get_contents ($sql_file);
        exec (
            'mariadb '.escapeshellarg($this->database).' < '.escapeshellarg($sql_file),
            $output,
            $status
        );
        if ($status>0) {
            $this->error_log (124,$sql_file.' '.implode(' ',$output));
            throw new \Exception ("SQL file '$sql_file' execution error");
            return false;
        }
        return $output;
    }

    private function footer ( ) {
        return '</ddcm>';
    }

    private function get_collections ($start,$end,$limit) {
        $what = 'collectionReport';
        $request = $this->report_request ($what,$start,$end,$limit);
        return $this->handle ($what, $request);
    }

    private function get_mandates ($start,$end,$limit) {
        $what = 'mandateReport';
        $request = $this->report_request ($what,$start,$end,$limit);
        return $this->handle ($what, $request);
    }

    private function handle ($what,$request) {
        $header             = $this->header($what.'Request');
        $sig                = $this->signature ($request);
        $footer             = $this->footer ();
        $postdata           = array ('xml'=>$header.$request.$sig.$footer);
        try {
            error_log(print_r ($postdata,true));
            $response       = $this->curl_post (RSM_URL,$postdata);
            //echo $response;
            $new            = simplexml_load_string ($response);
            $con            = json_encode ($new); //$new behaves like an object, but is actually a libxml resource 
            $response       = json_decode ($con,true); // decode to associative array
            return $response;
        }
        catch (\Exception $e) {
            $this->error_log (111,'curl POST failed: '.$e->getMessage());
            throw new \Exception ('curl POST failed');
            return false;
        }

    }

    private function header ($type) {
        $ret                = '<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE ddcm SYSTEM "apiDtd/'.$type.'.dtd">
<ddcm>';
        return $ret;
    }

    private function heartbeat_request ( ) {
        return $this->request_start('heartbeat').$this->request_end();
    }

    public function import ($start_date,$rowsm=0,$rowsc=0) {
        $this->execute (__DIR__.'/create_collection.sql');
        $this->execute (__DIR__.'/create_mandate.sql');
        $now = new \DateTime();
        $then = new \DateTime ($start_date);
        $then->modify ('first day of this month');
        while ($then<$now) {
            $start = $then->format ('01/m/Y');
            $end   = $then->format ('t/m/Y');
            $response = $this->get_mandates ($start,$end,$rowsm)['response'];
            if ($response['status']!='SUCCESS') {
                $this->error_log (123,'Mandate request was unsuccessful');
                throw new \Exception ('API error (mandate): '.$response['status']);
                return false;
            }
            echo "Received {$response['noOfRecords']} mandates [ $start thru $end ]\n";
            if ($response['noOfRecords'] > 0) {
                if ($response['noOfRecords'] == 1) { 
                    $data = array( $response['mandates']['mandate']);
                } else {
                    $data = $response['mandates']['mandate'];
                }
                $this->table_load (
                    $data,
                    'rsm_mandate',
                    $this->fieldsm
                );
            }
            $response = $this->get_collections ($start,$end,$rowsc)['response'];
            if ($response['status']!='SUCCESS') {
                $this->error_log (122,'Collection request was unsuccessful');
                throw new \Exception ('API error (collection): '.$response['status']);
                return false;
            }
            echo "Received {$response['noOfRecords']} collections [ $start thru $end ]\n";
            if ($response['noOfRecords'] > 0) {
                if ($response['noOfRecords'] == 1) {
                    $data = array($response['collections']['collection']);
                } else {
                    $data = $response['collections']['collection'];
                }
                $this->table_load (
                    $data,
                    'rsm_collection',
                    $this->fieldsc
                );
            }
            $then->modify ('+1 month');
        }
        $this->table_alter ('rsm_collection');
        $this->table_alter ('rsm_mandate');
        if (RSM_FILE_DEBOGON) {
            $this->debogon ();
        }
        $this->bogon_check ();
        $this->iscurrent_set ();
        $this->output_mandates ();
        $this->output_collections ();
    }

    public function insert_mandates ($mandates,&$bad=0,&$good=0)  {
        if (!count($mandates)) {
            fwrite (STDERR,"No mandates to insert\n");
            return true;
        }
        $what = 'setMandates';
        $body = "<mandates>";
        foreach ($mandates as $m) {
            if (trim($m['Type'])!='C') {
                throw new \Exception ("Currently treating an FLC record not of type 'C' (= create new customer) as an error");
                return false;
            }
            if (!array_key_exists($m['Freq'],$this->frequency)) {
                throw new \Exception ("Payment frequency '{$m['Freq']}' is not FLC standards compliant");
                return false;
            }
            $sortcode = str_replace ('-','',$m['Sortcode']);
            $csd = collection_startdate (gmdate('Y-m-d'),$m['PayDay']);
            $dt_csd = \DateTime::createFromFormat ('Y-m-d',$csd); // is there a better way of converting?
            $rsm_startdate = $dt_csd->format ('d/m/Y');

            $action = (strtolower($m['Type']) == 'c') ? 'N' : 'A'; // New, Amend, Delete
            //some optional elements commented out - but left here for reference!
            $body .= "<mandate>";
            // $body .= "<tradingName>".RSM_TRADING_NAME."</tradingName>"; // e.g. if account holder is business
            $body .= "<contactName>{$m['Name']}</contactName>"; // required if confirmation emails are enabled (whether or not mandatory)
            // $body .= "<address1></address1>"; // required if address checking enabled
            // $body .= "<address2></address2>";
            // $body .= "<address3></address3>";
            // $body .= "<town></town>";
            // $body .= "<postcode></postcode>";
            // $body .= "<phone></phone>";
            // $body .= "<email></email>"; // required if confirmation email is mandatory
            $body .= "<clientRef>{$m['ClientRef']}</clientRef>";
            $body .= "<accountName>{$m['Name']}</accountName>";
            $body .= "<accountNumber>{$m['Account']}</accountNumber>";
            $body .= "<sortCode>$sortcode</sortCode>";
            $body .= "<action>{$action}</action>";
            //$body .= "<ddRefNo></ddRefNo>";
            $body .= "<amount>{$m['Amount']}</amount>";
            $body .= "<frequency>{$this->frequency[$m['Freq']]}</frequency>";
            $body .= "<startDate>".$rsm_startdate."</startDate>";
            //$body .= "<mandateType>{$m[]}</mandateType>";
            //$body .= "<shortId>{$m[]}</shortId>";
            //$body .= "<endDate>{$m[]}</endDate>";
            $body .= "<paymentRef>{$m['Chances']}</paymentRef>";
            $body .= "</mandate>";
        }
        $body .= "</mandates>";
        $request = $this->request_start ($what).$body.$this->request_end();
        print_r($request); // send to logfile

        $response = $this->handle ($what, $request);

        $body = "";
        $mandates_array = [];
        if (is_array($response)) {
            $signature = $response['signature'];
            echo $signature."\n"; // dump to logfile
            $response = $response['response'];
            print_r ($response); // dump to logfile
            if (is_array($response) && array_key_exists('summary',$response)) {
                $good = $response['summary']['totalSuccessful'];
                $bad  = $response['summary']['totalFailed'];
                $mandates_array = $response['mandates']['mandate'];
                if (isset($mandates_array['status'])) { // special case when only one
                    $mandates_array = array($mandates_array);
                }
                foreach ($mandates_array as $mandate) {
                    // clientRef, status, error code(s) (if any) per line
                    $body .= $mandate['clientRef'].' '.$mandate['status']."\n";
                    if (isset($mandate['errors'])) {
                        $ocr = explode (BLOTTO_CREF_SPLITTER,$mandate['clientRef']) [0];
                        $body .= adminer('Supporters','original_client_ref','=',$ocr)."\n";
                        $error_array = $mandate['errors']['error'];
                        if (isset($error_array['code'])) {
                            $error_array[0] = $error_array;  // same as mandates above
                        }
                        foreach ($error_array as $errdetail) {
                            $body .= $errdetail['code'].' '.$errdetail['detail']."\n";
                        }
                    }
                }
            }
            else {
                $good = 0;
                $bad = count ($mandates);
            }
        }
        else {
            $good = 0;
            $bad = count ($mandates);
        }
        // send
        $subj = "RSM insert mandates for ".strtoupper(BLOTTO_ORG_USER).", $good good, $bad bad";
        mail(BLOTTO_EMAIL_WARN_TO, $subj, $body);

        // whatever happens we continue the build process; email alerts admins to problems
        // and we'll try again on next build.
        return true;
    }

    private function iscurrent_set ( )  {
        $this->execute (__DIR__.'/iscurrent.sql');
        $this->bogon_check ();
    }

    private function output_collections ( ) {
        $sql                = "INSERT INTO `".RSM_TABLE_COLLECTION."`\n";
        $sql               .= file_get_contents (__DIR__.'/select_collection.sql');
        $sql                = str_replace ('{{RSM_PAY_INTERVAL}}',RSM_PAY_INTERVAL,$sql);
        echo $sql;
        try {
            $this->connection->query ($sql);
            tee ("Output {$this->connection->affected_rows} collections\n");
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (121,'SQL insert failed: '.$e->getMessage());
            throw new \Exception ('SQL error');
            return false;
        }
    }

    private function output_mandates ( ) {
        $sql    = "INSERT INTO `".RSM_TABLE_MANDATE."`\n";
        $sql   .= file_get_contents (__DIR__.'/select_mandate.sql');
        $sql    = str_replace ('{{WHERE}}',"",$sql);
        echo $sql;
        try {
            $this->connection->query ($sql);
            tee ("Output {$this->connection->affected_rows} mandates\n");
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (120,'SQL insert failed: '.$e->getMessage());
            throw new \Exception ('SQL error');
            return false;
        }
    }

    public function player_new_tmp ($mandate,$db_live=null) {
        // Use API and insert the internal mandate
        $this->insert_mandates ([$mandate],$bad);
        if ($bad>0) {
           // The API did not create the mandate
           return null;
        }
        $crf = $this->connection->real_escape_string ($mandate['ClientRef']);
        // Insert the internal mandate
        $today = gmdate ('Y-m-d');
        $data = [
            [
                'status'              => 'PENDING',
// TODO: if table becomes persistent in the future we will have to be more clever than this
                'created'             => $today, // rsm_mandate is ephemeral; if this changes a bit next build, no sweat
                'updated'             => $today, // ditto
                'ddRefNo'             => uuid (), // ditto so anything unique will do until next build
                'clientRef'           => $mandate['ClientRef'],
                'name'                => $mandate['Name'],
                'sortcode'            => $mandate['Sortcode'],
                'account'             => $mandate['Account'],
                'frequency'           => $mandate['Freq'],
                'amount'              => $mandate['Amount'],
                'startDate'           => $mandate['StartDate'],
                'paymentReference'    => $mandate['Chances'],
            ]
        ];
        $this->table_load ($data,'rsm_mandate',$this->fieldsm);
        // Insert the blotto2 mandate
        $table  = RSM_TABLE_MANDATE;
        $sql    = "INSERT INTO `$table`\n";
        $sql   .= file_get_contents (__DIR__.'/select_mandate.sql');
        $sql    = str_replace ('{{WHERE}}',"WHERE `m`.`ClientRef`='$crf'",$sql);
        echo $sql;
        try {
            $this->connection->query ($sql);
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (119,'Find new mandate failed: '.$e->getMessage());
            throw new \Exception ('SQL error '.$e->getMessage());
            // The API created the mandate but other processes did not complete
            return false;
        }
        if ($db_live) {
            // Insert the live internal mandate
            $q = "
              INSERT INTO `$db_live`.`rsm_mandate`
              SELECT * FROM `rsm_mandate`
              WHERE `ClientRef`='$crf'
            ";
            try {
                $this->connection->query ($sql);
            }
            catch (\mysqli_sql_exception $e) {
                $this->error_log (118,'Copy new mandate live failed: '.$e->getMessage());
                throw new \Exception ('SQL error '.$e->getMessage());
                // The API created the mandate but other processes did not complete
                return false;
            }
            // Insert the live blotto2 mandate
            $q = "
              INSERT INTO `$db_live`.`$table`
              SELECT * FROM `$table`
              WHERE `ClientRef`='$crf'
            ";
            try {
                $this->connection->query ($sql);
            }
            catch (\mysqli_sql_exception $e) {
                $this->error_log (117,'Copy new mandate live failed: '.$e->getMessage());
                throw new \Exception ('SQL error '.$e->getMessage());
                // The API created the mandate but other processes did not complete
                return false;
            }
        }
        // TODO: cancel previous via API using $mandate[ClientRefPrevious]; in short term admin does it via provider dashboard
        // The API created the mandate and all other processes completed
        return true;
    }

    private function report_request ($what,$start,$end,$limit) {
        $filters      = '
    <filters>
      <startDate>'.$start.'</startDate>
      <endDate>'.$end.'</endDate>';
        if ($limit) {
            $filters .= '
      <noOfRows>'.$limit.'</noOfRows>';
        }
        $filters     .= '
    </filters>';
        return $this->request_start ($what).$filters.$this->request_end();
    }

    private function request_start ($cmd) {
        $ret = '
  <request>
    <command>'.$cmd.'</command>
    <username>'.RSM_USER.'</username>';
        return $ret;
    }

    private function request_end ( ) {
        $ret = '
    <timestamp>'.time().'</timestamp>
  </request>';
        return $ret;
    }

    private function setup ( ) {
        foreach ($this->constants as $c) {
            if (!defined($c)) {
                $this->error_log (116,"$c not defined");
                throw new \Exception ('Configuration error');
                return false;
            }
        }
        if (RSM_FILE_DEBOGON) {
            $this->bogon_file = RSM_FILE_DEBOGON;
            if (!is_readable(RSM_FILE_DEBOGON)) {
                $this->error_log (115,"Unreadable file '{$this->bogon_file}'");
                throw new \Exception ("Bogon file '{$this->bogon_file}' is not readable");
                return false;
            }
        }
        $sql                = "SELECT DATABASE() AS `db`";
        try {
            $db             = $this->connection->query ($sql);
            $db             = $db->fetch_assoc ();
            $this->database = $db['db'];
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (114,'SQL select failed: '.$e->getMessage());
            throw new \Exception ('SQL database error');
            return false;
        }
    }

    private function signature ($request) {
        $xml          = simplexml_load_string ($request,'SimpleXMLElement',LIBXML_NOBLANKS);
        $xmlarr       = explode ("\n",$xml->asXML());
        $noblanks     = trim ($xmlarr[1]);
        //echo "\n\n!!".$noblanks.RSM_PASSWORD."!!\n\n";
        $sig          = hash ('sha256',$noblanks.RSM_PASSWORD);
        $sig          = '
    <signature>'.$sig.'</signature>';
        return $sig;
    }

    private function table_alter ($table) {
        if ($table=='rsm_mandate') {
            $file = 'alter_mandate.sql';
        }
        elseif ($table=='rsm_collection') {
            $file = 'alter_collection.sql';
        }
        else {
            $this->error_log (113,"Internal error");
            throw new \Exception ("Table '$table' not recognised");
            return false;
        }
        $this->execute (__DIR__.'/'.$file);
    }

    private function table_load ($data,$tablename,$fields) {
        $sql                = "INSERT INTO ".$tablename." (`".implode('`, `', $fields)."`) VALUES\n";
        foreach ($data as $record) {
            $dbline         = [];
            foreach ($fields as $srcname=>$destname) {
                if (is_array($record[$srcname])) {
                    $record[$srcname] = '';
                }
                $dbline[]   = $this->connection->real_escape_string (trim($record[$srcname]));
            }
            $sql           .= "('".implode("','", $dbline)."'),\n";
        }
        $sql                = substr ($sql,0,-2);
        try {
            $this->connection->query ($sql);
            echo "Inserted {$this->connection->affected_rows} rows into `$tablename`\n";
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (112,'SQL insert failed: '.$e->getMessage());
            throw new \Exception ('SQL insert error');
            return false;
        }
    }

}

