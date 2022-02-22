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
        if (!$result=curl_exec($ch)) {
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
        //print_r ($postdata);
        $response           = $this->curl_post (RSM_URL,$postdata);
        //echo $response;
        $new                = simplexml_load_string ($response);
        $con                = json_encode ($new); //$new behaves like an object, but is actually a libxml resource 
        $response           = json_decode ($con,true); // decode to associative array
        return $response;
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
        $this->output_mandates ();
        $this->output_collections ();
    }

    public function insert_mandates ($mandates)  {
        if (!count($mandates)) {
            fwrite (STDERR,"No mandates to insert\n");
            return true;
        }
        // For now, we create the request and dump to the big log
        $what = 'setMandates';
        $body = "<mandates>";
        foreach ($mandates as $m) {
            if (trim($m['Type'])!='C') {
                throw new \Exception ('Currently treating an FLC record not of type "C" (= create new customer) as an error');
                return false;
            }

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
            $body .= "<sortCode>{$m['SortCode']}</sortCode>";
            $body .= "<action>{$action}</action>";
            //$body .= "<ddRefNo></ddRefNo>";
            $body .= "<amount>{$m['Amount']}</amount>";
            $body .= "<frequency>{$m['Freq']}</frequency>";
            $body .= "<startDate>".date('d/m/Y',collection_startdate(date('Y-m-d'),$m['PayDay']))."</startDate>";
            //$body .= "<mandateType>{$m[]}</mandateType>";
            //$body .= "<shortId>{$m[]}</shortId>";
            //$body .= "<endDate>{$m[]}</endDate>";
            $body .= "<paymentRef>{$m['Chances']}</paymentRef>";
            $body .= "</mandate>";
        }
        $body .= "</mandates>";
        $request = $this->request_start ($what).$body.$this->request_end();
        print_r($request); // send to logfile

//        return true; // TODO remove this in due course

        $response = $this->handle ($what, $request);

        /*
         ** this is here temporarily
            [status] => FAIL
            [summary] => Array
                (
                    [totalSubmitted] => 1
                    [totalSuccessful] => 0
                    [totalFailed] => 1
                )

            [mandates] => Array
                (
                    [mandate] => Array
                        (
                            [0] => Array
                                (
                                    [status] => FAIL
        NB that if only one mandate then there is no [0]; the next line is status
        */

        print_r ($response); // dump to logfile

        if (is_array($response) && array_key_exists('summary',$response)) {
            $good = $response['summary']['totalSuccessful'];
            $bad  = $response['summary']['totalFailed'];
        }
        else {
            $good = 0;
            $bad = count ($mandates);
        }
        $subj = "RSM insert mandates for ".strtolower(BLOTTO_ORG_USER).", $good good, $bad bad";
        $body = ;
        $mandates_array = $response['mandates']['mandate'];
        if (isset($mandates_array['status'])) { // special case when only one
            $mandates_array[0] = $mandates_array;
        }
        foreach ($mandates_array as $mandate) {
            // clientRef, status, error code(s) (if any) per line
            $body .= $mandate['clientRef'].' '.$mandate['status'];
            if (isset($mandate['errors'])) {
                // $body .= $mandate['errors']['error']['code'].' '.$mandate['errors']['error']['detail'];
                // for now until we know more about the format of errors we just dump it.
                // because if there's more than one error it is probably an array - similar to 
                // mandates_array above
                $body .= "\n".print_r($mandate['errors'], true);
            }
            $body .= "\n";
        }
        // send
        mail(BLOTTO_EMAIL_WARN_TO, $subj, $body);

        // whatever happens we continue the build process; email alerts admins to problems
        // and we'll try again on next build.
        return true;
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
            $this->error_log (120,'SQL insert failed: '.$e->getMessage());
            throw new \Exception ('SQL error');
            return false;
        }
    }

    private function output_mandates ( ) {
        $sql                = "INSERT INTO `".RSM_TABLE_MANDATE."`\n";
        $sql               .= file_get_contents (__DIR__.'/select_mandate.sql');
        echo $sql;
        try {
            $this->connection->query ($sql);
            tee ("Output {$this->connection->affected_rows} mandates\n");
        }
        catch (\mysqli_sql_exception $e) {
            $this->error_log (119,'SQL insert failed: '.$e->getMessage());
            throw new \Exception ('SQL error');
            return false;
        }
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
                $this->error_log (118,"$c not defined");
                throw new \Exception ('Configuration error');
                return false;
            }
        }
        if (RSM_FILE_DEBOGON) {
            $this->bogon_file = RSM_FILE_DEBOGON;
            if (!is_readable(RSM_FILE_DEBOGON)) {
                $this->error_log (117,"Unreadable file '{$this->bogon_file}'");
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
            $this->error_log (116,'SQL select failed: '.$e->getMessage());
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
            $this->error_log (115,"Internal error");
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
            $this->error_log (114,'SQL insert failed: '.$e->getMessage());
            throw new \Exception ('SQL insert error');
            return false;
        }
    }

}

