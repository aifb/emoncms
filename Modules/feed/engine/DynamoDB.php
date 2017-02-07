<?php
require 'vendor/autoload.php';

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;


class DynamoDB
{
    private $log;

    private $writebuffer = array();

    private $dynamodb;

    /**
     * Constructor.
     *
     * @api
    */
    public function __construct()
    {
        $this->log = new EmonLogger(__FILE__);

        date_default_timezone_set('UTC');
        $sdk = new Aws\Sdk([
            'endpoint'   => 'http://localhost:8000',
            'region'   => 'us-west-2',
            'version'  => 'latest',
            'credentials' => [
                'key' => 'not-a-real-key',
                'secret' => 'not-a-real-secret',
            ]
        ]);
        $this->dynamodb = $sdk->createDynamoDb();

   }

// #### \/ Below are required methods
    /**
     * Create feed.
     *
     * @param integer $feedid The id of the feed to be created
     * @param array $options for the engine
    */
    public function create($feedid,$options=NULL)
    {

        $feedname = "feed_".trim($feedid)."";

        $params = [
            'TableName' => $feedname,
            'KeySchema' => [
                [
                    'AttributeName' => 'week_index',
                    'KeyType' => 'HASH'  // Partition key
                ],
                [
                    'AttributeName' => 'time',
                    'KeyType' => 'RANGE' // Sort Key
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => 'week_index',
                    'AttributeType' => 'N'
                ],
                [
                    'AttributeName' => 'time',
                    'AttributeType' => 'N'
                ],
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 10,
                'WriteCapacityUnits' => 10
            ]
        ];

        try {
            $result = $this->dynamodb->createTable($params);
            //echo 'Created table.  Status: ' .
            //    $result['TableDescription']['TableStatus'] ."\n";

            return true;
        } catch (DynamoDbException $e) {
            echo "Unable to create table:\n";
            echo $e->getMessage() . "\n";

            return false;
        }

    }

    /**
     * Delete feed
     *
     * @param integer $feedid The id of the feed to be deleted
    */
    public function delete($feedid)
    {
        $feedname = "feed_".trim($feedid)."";

        // Delete Table for the given feed id.
        $params_1 = [
            'TableName' => $feedname
        ];

        try {
            $result = $this->dynamodb->deleteTable($params_1);
            echo "Deleted Table " . $feedname . "\n";
        } catch(DynamoDbException $e) {
            echo "Unable to delete table: \n";
            echo $e->getMessage() . "\n";
        }

        // delete the item with given feed id from 'last_values' table
        $marshaler = new Marshaler();
        $key = $marshaler->marshalJson('
            {
                "feed_id": ' . $feedid . '
            }
        ');

        $params_2 = [
            'TableName' => 'last_values',
            'Key' => $key
        ];

        try {
            $result = $this->dynamodb->deleteItem($params_2);
            echo "Deleted item from Table last_values\n";

        } catch (DynamoDbException $e) {
            echo "Unable to delete item:\n";
            echo $e->getMessage() . "\n";
        }

    }

    /**
     * Gets engine metadata
     *
     * @param integer $feedid The id of the feed to be created
    */
    public function get_meta($feedid)
    {
        $meta = new stdClass();
        $meta->id = $feedid;
        $meta->start_time = 0; // tbd
        $meta->nlayers = 1;
        $meta->npoints = -1;
        $meta->interval = 1;
        return $meta;
    }

    /**
     * Returns engine occupied size in bytes
     *
     * @param integer $feedid The id of the feed of which table size is to be returned.
    */
    public function get_feed_size($feedid)
    {
        $num_items = $this->get_npoints($feedid);
        // multiply $num_items with item size in bytes.
        return $num_items * 8;
    }

    /**
     * Adds a data point to the feed
     *
     * @param integer $feedid The id of the feed to add to
     * @param integer $time The unix timestamp of the data point, in seconds
     * @param float $value The value of the data point
     * @param array $arg optional padding mode argument
    */
    public function post($feedid,$time,$value,$arg=null)
    {
        $feedname = "feed_".trim($feedid)."";
        $week_ind = ceil( (float) $time / 604800 );

        $marshaler = new Marshaler();
        $item = $marshaler->marshalJson('
            {
                "week_index": ' . $week_ind . ',
                "time": ' . $time . ',
                "value": ' . $value . '
            }
        ');

        $params = [
            'TableName' => $feedname,
            'Item' => $item
        ];

        try {
            $result = $this->dynamodb->putItem($params);
        } catch (DynamoDbException $e) {
            echo "Unable to add item:\n";
            echo $e->getMessage() . "\n";
            return false;
        }
        // add this data also to the 'last_values' table.
        $this->put_lastvalue($feedid, $time, $value);
    }

    /*
     *  Adds the given data (@time and @value) of the feed with the given @feedid to the 'last_values' table.
     *  If there is already item in the table with the primary key = given feed id, then attribute values
     *  of that item will be overwritten.
     */
    private function put_lastvalue($feedid,$time,$value) {
        $marshaler = new Marshaler();

        $item = $marshaler->marshalJson('
            {
                "feed_id": ' . $feedid . ',
                "time": ' . $time . ',
                "value": ' . $value . '
            }
        ');

        $params = [
            'TableName' => 'last_values',
            'Item' => $item
        ];

        try {
            $result = $this->dynamodb->putItem($params);
            return $value;
        } catch (DynamoDbException $e) {
            echo "Unable to add item:\n";
            echo $e->getMessage() . "\n";
        }
    }

    /**
     * Updates a data point in the feed
     *
     * @param integer $feedid The id of the feed to add to
     * @param integer $time The unix timestamp of the data point, in seconds
     * @param float $value The value of the data point
    */
    public function update($feedid,$time,$value)
    {
      return $this->post($feedid,$time,$value);
    }

    /**
     * Get array with last time and value from a feed
     *
     * @param integer $feedid The id of the feed
    */
    public function lastvalue($feedid)
    {
        $marshaler = new Marshaler();
        $key = $marshaler->marshalJson('
            {
                "feed_id": ' . $feedid . '
            }
        ');

        $params = [
            'TableName' => 'last_values',
            'Key' => $key
        ];

        try {
            $result = $this->dynamodb->getItem($params);
            $array = array(
                'time' =>  $result["Item"]["time"]["N"],
                'value' => $result["Item"]["value"]["N"]
            );
            return $array;

        } catch (DynamoDbException $e) {
            echo "Unable to get item:\n";
            echo $e->getMessage() . "\n";
            return false;
        }
    }

    /**
     * Return the data for the given timerange
     *
     * @param integer $feedid The id of the feed to fetch from
     * @param integer $start The unix timestamp in ms of the start of the data range
     * @param integer $end The unix timestamp in ms of the end of the data range
     * @param integer $interval The number os seconds for each data point to return (used by some engines)
     * @param integer $skipmissing Skip null values from returned data (used by some engines)
     * @param integer $limitinterval Limit datapoints returned to this value (used by some engines)
    */
    public function get_data($feedid,$start,$end,$interval,$skipmissing,$limitinterval)
    {
        $start = intval($start/1000);
        $end = intval($end/1000);
        $interval= (int) $interval;
        $feedname = "feed_".trim($feedid)."";

        // Minimum interval
        if ($interval<1) $interval = 1;
        // Maximum request size
        $req_dp = round(($end-$start) / $interval);
        if ($req_dp>8928)
            return array("success"=>false,
                         "message"=>"request datapoint limit reached (8928), increase request interval or time range, requested datapoints = $req_dp"
                        );

        $data = array();
        $atime = 0;
        $marshaler = new Marshaler();
        $time = $start;
        $last_item_time = -1;
        $num_of_items = 60000;
        $dat_points = array();

        while ($time<=$end)
        {

            if ($time >= $last_item_time) {
                unset($dat_points);
                $dat_points = $this->query($time, $feedname, $num_of_items);
                $count = intval ($dat_points['Count']);
                $last_item_time = $count ? $marshaler->unmarshalValue($dat_points['Items'][$count-1]['time']) : ($time + $num_of_items - 1);

                while ($count == 0 && ($last_item_time + $num_of_items) < ($time + $interval)) {
                    unset($dat_points);
                    $dat_points = $this->query($last_item_time+1, $feedname, $num_of_items);
                    $count = intval ($dat_points['Count']);

                    $last_item_time = $count ? $marshaler->unmarshalValue($dat_points['Items'][$count-1]['time']) : ($last_item_time + $num_of_items);
                }

                if ($count == 0 && ($last_item_time + $num_of_items) >= ($time + $interval) ) {
                    $dat_points = $this->query($last_item_time+1, $feedname, $num_of_items);
                    $last_item_time = $count ? $marshaler->unmarshalValue($dat_points['Items'][$count-1]['time']) : ($last_item_time + $num_of_items);
                }

            }

            $index = $this->binarysearch($time, $dat_points);

            if ($index != -1) {
                $dptime = $marshaler->unmarshalValue($dat_points['Items'][$index]['time']);
                $value = null;

                $lasttime = $atime;
                $atime = $time;

                if ($limitinterval) {
                    $diff = abs($dptime-$time);
                    if ($diff<$interval) {
                        $value = $marshaler->unmarshalValue($dat_points['Items'][$index]['value']);
                    }
                } else {
                    $value = $marshaler->unmarshalValue($dat_points['Items'][$index]['value']);
                    $atime = $marshaler->unmarshalValue($dat_points['Items'][$index]['time']);
                }

                if ($atime!=$lasttime) {
                    if ($value!==null || $skipmissing===0) $data[] = array($atime*1000,$value);
                }
            }
            $time += $interval;
        }

        return $data;

    }

    public function export($feedid,$start)
    {
        $feedid = (int) $feedid;
        $start = (int) $start;

        $feedname = "feed_".trim($feedid)."";

        // There is no need for the browser to cache the output
        header("Cache-Control: no-cache, no-store, must-revalidate");

        // Tell the browser to handle output as a csv file to be downloaded
        header('Content-Description: File Transfer');
        header("Content-type: application/octet-stream");
        header("Content-Disposition: attachment; filename={$feedname}");

        header("Expires: 0");
        header("Pragma: no-cache");

        // Write to output stream
        $fh = @fopen( 'php://output', 'w' );
    }

    public function csv_export($feedid,$start,$end,$outinterval,$usertimezone)
    {
        global $csv_decimal_places, $csv_decimal_place_separator, $csv_field_separator;

        require_once "Modules/feed/engine/shared_helper.php";
        $helperclass = new SharedHelper();

        $feedid = (int) $feedid;
        $start = (int) $start;
        $end = (int) $end;
        $outinterval = (int) $outinterval;

        if ($outinterval<1)
            $outinterval = 1;
        $dp = ceil(($end - $start) / $outinterval);
        $end = $start + ($dp * $outinterval);
        if ($dp<1)
            return false;
        $feedname = "feed_".trim($feedid)."";

        $interval = ($end - $start) / $dp;

        // Ensure that interval request is less than 1
        // adjust number of datapoints to request if $interval = 1;
        if ($interval<1) {
            $interval = 1;
            $dp = ($end - $start) / $interval;
        }

        $data = array();

        $time = 0;

        // There is no need for the browser to cache the output
        header("Cache-Control: no-cache, no-store, must-revalidate");

        // Tell the browser to handle output as a csv file to be downloaded
        header('Content-Description: File Transfer');
        header("Content-type: application/octet-stream");
        $filename = $feedid.".csv";
        header("Content-Disposition: attachment; filename={$filename}");

        header("Expires: 0");
        header("Pragma: no-cache");

        // Write to output stream
        $exportfh = @fopen( 'php://output', 'w' );

        $marshaler = new Marshaler();

        //query data for the first time
        $prev_week_ind = ceil( (float) $start / 604800 );
        $dat_points = $this->query($prev_week_ind, $feedname);

        for ($i=0; $i<$dp; $i++)
        {
            $t = $start + ( $i * $interval );
            $week_ind = ceil( (float) $t / 604800 );

            // if week index changed, it means datapoints are in another physical partition,
            // therefore we access them with the new hash key (=new week index).
            if ($week_ind != $prev_week_ind) {
                $dat_points = $this->query($week_ind, $feedname);
                $prev_week_ind = $week_ind;
            }

            if (intval ($dat_points['Count']) > 0) {
                $index = $this->binarysearch($t, $dat_points);

                if ($index != -1) {
                    $value = $marshaler->unmarshalValue($dat_points['Items'][$index]['value']);
                    $last_time = $time;
                    $time = $marshaler->unmarshalValue($dat_points['Items'][$index]['time']);
                    $timenew = $helperclass->getTimeZoneFormated($time,$usertimezone);
                    // $last_time = 0 only occur in the first run
                    if (($time!=$last_time && $time>$last_time) || $last_time==0) {
                        fwrite($exportfh, $timenew.$csv_field_separator.number_format($value,$csv_decimal_places,$csv_decimal_place_separator,'')."\n");
                    }
                }
            }

         }

    }

// #### /\ Above are required methods


// #### \/ Below are buffer write methods

    // Insert data in post write buffer, parameters like post()
    // $this->writebuffer[$feedid] keeps items, which are grouped in arrays of length 25,
    // except the last element. it has the form:
    // $this->writebuffer[$feedid] -> [ [25 items], [next 25 items], ......, [e.g. 13 items] ]
    // In bulk post, we publish each of those elements (array of length 25) in one 'BatchWriteItem' operation
    // where 25 is the maximal size of items to be posted within one BatchWriteItem operation.
    public function post_bulk_prepare($feedid,$timestamp,$value,$arg=null)
    {
        $feedid = (int) $feedid;
        $timestamp = (int) $timestamp;
        $value = (float) $value;

        $filename = "feed_".trim($feedid)."";
        $npoints = $this->get_npoints($feedid);

        if (!isset($this->writebuffer[$feedid])) {
            $this->writebuffer[$feedid] = [ [] ];
        }

        // If there is data then read last value
        if ($npoints >= 1)
        {
            static $lastvalue_static_cache = array(); // Array to hold the cache
            if (!isset($lastvalue_static_cache[$feedid])) { // Not set, cache it from file data
                $lastvalue_static_cache[$feedid] = $this->lastvalue($feedid);
            }
            if ($timestamp<=$lastvalue_static_cache[$feedid]['time']) {
                // if data is in past, its not supported, could call update here to fix on file before continuing
                // but really this should not happen for past data has process_feed_buffer uses update for that.
                $this->log->warn("post_bulk_prepare() data in past, nothing saved.  feedid=$feedid timestamp=$timestamp last=".$lastvalue_static_cache[$feedid]['time']." value=$value");
                return $value;
            }
        }

        // Get index of the last element of the array '$this->writebuffer[$feedid]'
        end($this->writebuffer[$feedid]);
        $last_elem_ind = key($this->writebuffer[$feedid]);

        //create an item
        $week_ind = ceil( (float) $timestamp / 604800 );
        $item = [
            'Item' => [
                'week_index' => ['N' => $week_ind],
                'time' => ['N' => $timestamp],
                'value' => ['N' => $value]
            ]
        ];

        $put_req = array('PutRequest' => $item);
        array_push($this->writebuffer[$feedid][$last_elem_ind], $put_req);
        // remember, writebuffer[$feedid] has the form [ [25 items], [next 25 items], ......, [e.g. 13 items] ]
        // check if the last element of the writebuffer[$feedid] reached the length of 25 elements, if yes, then
        // put empty array as a next element, resulting in -> [ [25 items], [next 25 items], ..., [25 items], [ ] ].
        if (sizeof ($this->writebuffer[$feedid][$last_elem_ind]) == 25) {
            $this->writebuffer[$feedid][$last_elem_ind+1] = [];
        }

        $lastvalue_static_cache[$feedid] = array('time'=>$timestamp,'value'=>$value); // Set static cache last value
        return $value;
    }

    // Saves post buffer to engine in bulk
    // Writing data in larger blocks saves reduces disk write load
    public function post_bulk_save()
    {
        $byteswritten = 0;

        foreach ($this->writebuffer as $feedid=>$data)
        {
            $tableName = "feed_".trim($feedid)."";
            $batch_count = sizeof( $data );

            for ($i = 0; $i < $batch_count; $i++) {
                $elem_count = sizeof($data[$i]);
                if ( $i == ($batch_count - 1) && $elem_count == 0 ) break;

                $param = [
                    "RequestItems" =>  [
                        $tableName => $data[$i]
                    ]
                ];


                $response = $this->dynamodb->batchWriteItem($param);

                // handle UnprocessedItems.
                $unproc_el_size = sizeof($response["UnprocessedItems"]);
                while ($unproc_el_size != 0) {
                    $param = [
                        "RequestItems" =>  [
                            $tableName => $response["UnprocessedItems"][$tableName]
                        ]
                    ];

                    $response = $this->dynamodb->batchWriteItem($param);
                    $unproc_el_size = sizeof($response["UnprocessedItems"]);
                }

                $byteswritten += $elem_count * 8;
                $lb_item = $data[$i][$elem_count - 1]['PutRequest']['Item']; // 'lb' = last batch
                $this->put_lastvalue( $feedid, $lb_item['time']['N'], $lb_item['value']['N'] );

                $this->clearBuffer($feedid, $i);
            }
            unset($this->writebuffer[$feedid]);
        }

        $this->writebuffer = array(); // Clear writebuffer

        return $byteswritten;

    }



// #### \/ Bellow are engine private methods

    /**
     * Returns item count in the feed.
     *
     * @param integer $feedid The id of the feed which item count is to be returned.
    */
    public function get_npoints($feedid)
    {
        $feedname = "feed_".trim($feedid)."";

        $params = [
            'TableName' => $feedname
        ];

        try {
            $result = $this->dynamodb->describeTable($params);
            return  intval( $result['Table']['ItemCount'] );
        } catch (DynamoDbException $e) {
            echo "Unable to create table:\n";
            echo $e->getMessage() . "\n";
        }

        return -1;
    }


    public function query($time, $feedname, $num_of_elems) {
        // compute week_ind from the given @time
        $week_ind = ceil( (float) $time / 604800 );

        $marshaler = new Marshaler();
        $eav = $marshaler->marshalJson('
            {
                ":w_i": ' . $week_ind . ',
                ":t": ' . $time . '
            }
        ');

        $params = [
            'TableName' => $feedname,
            'ProjectionExpression' => '#tm, #vl',
            'KeyConditionExpression' => "week_index = :w_i and #tm >= :t",
            'ExpressionAttributeNames' => ['#tm' => 'time', '#vl' => 'value'],
            'ExpressionAttributeValues' => $eav,
            'Limit' => $num_of_elems
        ];

        try {
            $result = $this->dynamodb->query($params);

            return $result;
        } catch (DynamoDbException $e) {
            echo "Unable to query:\n";
            echo $e->getMessage() . "\n";
        }

    }


    /**
    * Implements binary search on given list of data points. The
    * data point with time == $time is searched. This implementation
    * is different than the ordinary binary search in that sense that
    * the search always (unless the list is empty) returns one data point as a result.
    * If the list does not contain data point with the given time, then the nearest
    * data point (time as metric) is choosen and returned.
    *
    * @param list of items $dat_points Within this list search takes place.
    * @param integer $time     Data point with this time,
    *                       or if no such data point exists, the one with the nearest
    *                       unixtime is to be found.
    *
    * @return index of the found data point in the list.
    */
    private function binarysearch($time, $dat_points)
    {
        $marshaler = new Marshaler();
        $count = intval ($dat_points['Count']);

        if ($count == 0) return -1;

        $left = 0;
        $right = $count - 1;

        if ($time <= $marshaler->unmarshalValue($dat_points['Items'][$left]['time'])) {
            return $left;
        } else if ($time >= $marshaler->unmarshalValue($dat_points['Items'][$right]['time'])) {
            return $right;
        }

        while ($left <= $right)
        {
            // Get the value in the middle of our range
            $mid = $left + floor( ($right - $left) / 2 );

            $mid_time = $marshaler->unmarshalValue($dat_points['Items'][$mid]['time']);

            // If it is the value we want then exit
            if ($time == $mid_time) return $mid;

            if ($left == $right) {
                if ($left == 0) return $left;
                else if ($right == $count - 1) return $right;

                // see notes, why it is necessary to check for two neighbors and middle point as potential return values.
                return $this->findNext($time, $dat_points, $mid);
            }


            if ($mid_time > $time)
                $right = $mid - 1;
            else
                $left = $mid + 1;

        }

        // for the case when '$right < $left' holds.
        return $this->findNext($time, $dat_points, $mid);

    }

    /**
     * This function is called by the function 'binarysearch'
     * for the case, when data point with the requested unixtime
     * is not present in database. In this case, the nearest data
     * point is choosen (within this function) as a result of binarysearch.
     *
     * @assert when this function is called (caller = binarysearch function), in caller function following conditions
     * hold.
     * 1. $left == $mid == $right;
     * 2. requested data point with unixtime == $time is not present in database.
     *
     * There are 3 possible data points which are nominated (with indexes $mid, $mid-1 and $mid+1) for being the
     * nearest point for the given unixtime. Those data points are examined in this function and index of the nearest
     * data point is returned as a result.
     */
    private function findNext($time, $dat_points, $mid)
    {
        $marshaler = new Marshaler();
        $count = intval($dat_points['Count']);
        $mid_it = $marshaler->unmarshalValue($dat_points['Items'][$mid]['time']);

        if ($mid == 0) $prev_it = $mid_it; else  $prev_it = $marshaler->unmarshalValue($dat_points['Items'][$mid-1]['time']);
        if ($mid == ($count - 1) ) $next_it = $mid_it; else $next_it = $marshaler->unmarshalValue($dat_points['Items'][$mid+1]['time']);

        $mid_it = abs($mid_it - $time);
        $prev_it = abs($prev_it - $time);
        $next_it = abs($next_it - $time);

        $res = -1;

        if ($mid_it < $prev_it) {
            if ($mid_it < $next_it) $res = $mid; else $res = $mid + 1;
        } else {
            if ($prev_it < $next_it) $res = $mid - 1; else $res = $mid + 1;
        }

        return $res;

    }
    /**
     * Clears an element of "writebuffer[$feedid]" with the given index.
     * writebuffer[$feedid] has the form [[25 items], [25 items], ..., [e.g. 13 items]]
     *
     */
    private function clearBuffer($feedid, $index) {

        foreach($this->writebuffer[$feedid][$index] as $key=>$item) {
            unset($this->writebuffer[$feedid][$index][$key]["PutRequest"]["Item"]["week_index"]["N"]);
            unset($this->writebuffer[$feedid][$index][$key]["PutRequest"]["Item"]["time"]["N"]);
            unset($this->writebuffer[$feedid][$index][$key]["PutRequest"]["Item"]["value"]["N"]);
            unset($this->writebuffer[$feedid][$index][$key]["PutRequest"]["Item"]["week_index"]);
            unset($this->writebuffer[$feedid][$index][$key]["PutRequest"]["Item"]["time"]);
            unset($this->writebuffer[$feedid][$index][$key]["PutRequest"]["Item"]["value"]);
            unset($this->writebuffer[$feedid][$index][$key]["PutRequest"]["Item"]);
            unset($this->writebuffer[$feedid][$index][$key]["PutRequest"]);
            unset($this->writebuffer[$feedid][$index][$key]);
        }
        unset($this->writebuffer[$feedid][$index]);
    }

}
