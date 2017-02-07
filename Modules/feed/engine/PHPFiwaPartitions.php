<?php

/**
 * This class contains an implementation of new engine, called PHPFiwaPartitions. It is customized version of 'PHPFiwa', where data is
 * spread over partitions (files) instead of storing all data points in a single file. The main motivation behind implementing this
 * new engine was to make 'PHPFiwa' more efficient with respect to the huge data volumes which can contain large gaps.
 */
class PHPFiwaPartitions
{
    private $dir = "/var/lib/phpfiwapartitions/";
    private $log;
    const WEEK_NPOINTS = 604800;

    /**
     * Constructor.
     *
     * @api
     */
    public function __construct($settings)
    {
        if (isset($settings['datadir'])) $this->dir = $settings['datadir'];
        $this->log = new EmonLogger(__FILE__);
    }

    /**
     * Create feed
     *
     * @param integer $feedid The id of the feed to be created
     */
    public function create($id,$options)
    {
        $interval = (int) $options['interval'];
        if ($interval<1) $interval = 1;

        // Check to ensure we dont overwrite an existing feed
        if (!$meta = $this->get_meta($id))
        {
            $this->log->info("PHPFIWAPartitions:create creating feed id=$id");
            // Set initial feed meta data
            $meta = new stdClass();
            $meta->id = $id;
            $meta->nlayers = 0;

            if ($interval == 1 || $interval==5 || $interval==10 || $interval==15 || $interval==20 || $interval==30) {
                $meta->nlayers = 4;
                $meta->npoints = array(0,0,0,0);
                $meta->interval = array($interval,60,600,3600);
            }

            if ($interval==60 || $interval==120 || $interval==300) {
                $meta->nlayers = 3;
                $meta->npoints = array(0,0,0);
                $meta->interval = array($interval,600,3600);
            }

            if ($interval==600 || $interval==1200 || $interval==1800) {
                $meta->nlayers = 2;
                $meta->npoints = array(0,0);
                $meta->interval = array($interval,3600);
            }

            if ($interval==3600) {
                $meta->nlayers = 1;
                $meta->npoints = array(0);
                $meta->interval = array($interval);
            }

            // If interval is outside of the allowed layer intervals
            if ($meta->nlayers==0) return false;

            // Save meta data
            $this->create_meta($id,$meta);

        }

        $feedname = "$id.meta";
        $feed_dir = $this->dir . "$id/";
        if (file_exists($feed_dir.$feedname)) return true;
        return false;
    }


    /**
     * Adds a data point to the feed
     *
     * @param integer $feedid The id of the feed to add to
     * @param integer $time The unix timestamp of the data point, in seconds
     * @param float $value The value of the data point
     */
    public function post($id,$timestamp,$value,$arg=null)
    {
        $this->log->info("PHPFiwaPartitions:post id=$id timestamp=$timestamp value=$value");

        $id = (int) $id;
        $timestamp = (int) $timestamp;
        $value = (float) $value;

        $now = time();
        $start = $now-(3600*24*365*5); // 5 years in past
        $end = $now+(3600*48);         // 48 hours in future

        if ($timestamp<$start || $timestamp>$end) {
            $this->log->warn("PHPFiwaPartitions:post timestamp out of range");
            return false;
        }

        $layer = 0;

        // If meta data file does not exist then exit
        if (!$meta = $this->get_meta($id)) {
            $this->log->warn("PHPFiwaPartitions:post failed to fetch meta id=$id");
            return false;
        }

        $timestamp = floor( $timestamp / $meta->interval[$layer] ) * $meta->interval[$layer]; // rounding.
        $file_ind  = floor( $timestamp / (self::WEEK_NPOINTS * $meta->interval[$layer]) ) * $meta->interval[$layer];
        //"$file_ind * 604800" = starting timestamp of the corresponding file.
        $point_pos = 1 + floor( ($timestamp - $file_ind * self::WEEK_NPOINTS) / $meta->interval[$layer] );


        $result = $this->update_layer($meta, $layer, $point_pos, $timestamp, $value, $file_ind);


    }

    private function update_layer($meta, $layer, $point_pos, $timestamp, $value, $file_ind)
    {
        // file name of partition has the form: "<layer>_<file_index>", e.g. "1_10" -> layer 1, file_ind = 10.
        // Partition begins with week_ind = file_ind, e.g. in above example, first timestamp in that partition would be 10*604800 (=6048000).
        $file_name = $layer . "_" . $file_ind;
        $file_path = $this->dir . $meta->id . "/" . $file_name;

        $fh = fopen($file_path, 'c+');
        if (!$fh) {
            $this->log->warn("PHPFiwa partitions:update_layer could not open data file layer $layer id=".$meta->id . " file_ind " . $file_ind);
            return false;
        }

        if (!flock($fh, LOCK_EX)) {
            $this->log->warn("PHPFiwa partitions:update_layer data file for layer=$layer feedid=".$meta->id." is locked by another process");
            fclose($fh);
            return false;
        }

        clearstatcache($file_path);
        // If this is a new feed then set the npoints to 0.
        if ( filesize($file_path) == 0 ) {
            fwrite($fh, pack("I", 0)); // the first element of the file is npoints in that file.
            fseek($fh, 0);
        }

        // 1) Write padding
        $tmp = unpack("I", fread($fh, 4));
        $last_point = $tmp[1];
        $padding = ($point_pos - $last_point)-1;


        if ($padding>0) {
            if ($this->write_padding($fh,$meta->npoints[$layer],$padding)===false)
            {
                // Npadding returned false = max block size was exeeded
                $this->log->warn("PHPFiwa partition:update_layer padding max block size exeeded $padding id=".$meta->id);
                return false;
            }
        }

        // 2) Write new datapoint
        fseek($fh,4 * $point_pos);
        if (!is_nan($value)) fwrite($fh,pack("f",$value)); else fwrite($fh,pack("f",NAN));


        if ($point_pos > $last_point)
        {
            $meta->npoints[$layer] = $point_pos;
            fseek($fh, 0); fwrite($fh, pack("I", $point_pos));
            $this->create_meta($meta->id,$meta);
        }

        // 3) Averaging
        $layer++;

        if( $layer < $meta->nlayers )
        {
            $timestamp_avl = floor($timestamp / $meta->interval[$layer]) * $meta->interval[$layer]; // rounding.
            $file_ind_avl = floor( $timestamp_avl / (self::WEEK_NPOINTS * $meta->interval[$layer]) ) * $meta->interval[$layer];
            $point_pos_avl = 1 + floor( ($timestamp_avl - $file_ind_avl * self::WEEK_NPOINTS) / $meta->interval[$layer] ); // "+1" because, pos = 0 contains number of points in that partition.

            $point_in_avl = ($timestamp - $timestamp_avl) / $meta->interval[$layer-1];
            $first_point = $point_pos - $point_in_avl;

            // Read in points
            fseek($fh, 4*$first_point);
            $d = fread($fh, 4 * ($point_in_avl+1));
            $count = strlen($d)/4;
            $d = unpack("f*",$d);
            fclose($fh);

            // Calculate average of points
            $sum_count = 0;
            $sum = 0.0;

            $i=0;
            while ($count--) {
                $i++;
                if (is_nan($d[$i])) continue;   // Skip unknown values
                $sum += $d[$i];                 // Summing
                $sum_count ++;
            }

            if ($sum_count>0) {
                $average = $sum / $sum_count;
            } else {
                $average = NAN;
            }

            $meta = $this->update_layer($meta,$layer,$point_pos_avl,$timestamp_avl,$average, $file_ind_avl);
        }

        return $meta;
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

    }

    /**
     * Return the data for the given timerange
     *
     * @param integer $feedid The id of the feed to fetch from
     * @param integer $start The unix timestamp in ms of the start of the data range
     * @param integer $end The unix timestamp in ms of the end of the data range
     * @param integer $dp The number of data points to return (used by some engines)
    */
    //CHAVEIRO: this method is deprecated
    public function get_data_basic($feedid,$start,$end,$dp)
    {

    }


    public function get_data($feedid,$start,$end,$outinterval,$skipmissing,$limitinterval)
    {
        $feedid = intval($feedid);
        $start = intval($start/1000);
        $end = intval($end/1000);
        $outinterval = (int) $outinterval;

        if ($end < $start) return false;
        if (!$meta = $this->get_meta($feedid)) return false;

        if ($outinterval<$meta->interval[0]) $outinterval = $meta->interval[0];

        // 1) Find nearest layer with interval less than request interval
        $layer = 0;
        if ($meta->nlayers>1 && $outinterval >= $meta->interval[1]) $layer = 1;
        if ($meta->nlayers>2 && $outinterval >= $meta->interval[2]) $layer = 2;
        if ($meta->nlayers>3 && $outinterval >= $meta->interval[3]) $layer = 3;

        // 2) find start and end partition, where iteration will take place.
        $denominator = self::WEEK_NPOINTS * $meta->interval[$layer];
        $start_file_ind = floor( $start / $denominator ) * $meta->interval[$layer];
        $end_file_ind = floor( $end / $denominator ) * $meta->interval[$layer];
        $start_pos = 1 + ceil( ($start - $start_file_ind * self::WEEK_NPOINTS) / $meta->interval[$layer] );
        $end_pos = 1 + ceil( ($end - $end_file_ind * self::WEEK_NPOINTS) / $meta->interval[$layer] );
        $data = array();
        $current_ind = $start_file_ind;
        $dp_in_range = 0;
        $first_point_pos = $start_pos - 1;
        $time_basis = $start;
        $end_time = $end;

        while ($current_ind <= $end_file_ind) {
            $file_path = $this->dir . $meta->id . "/" . $layer . "_" . $current_ind;
            $cur_ind_start_time = $current_ind * self::WEEK_NPOINTS;
            $data_exist = 1;
            $npoints = 0;

            if (!file_exists($file_path) || filesize($file_path) <= 4) $data_exist = 0;


            if (!$data_exist) {
                if ($skipmissing == 1) {
                    $current_ind += $meta->interval[$layer];
                    $first_point_pos = 0;
                    $time_basis = $current_ind * self::WEEK_NPOINTS;
                    continue;
                }
            } else {
                $fh = fopen($file_path, 'rb');
                $tmp = unpack("I", fread($fh, 4));
                $npoints = $tmp[1];
            }


            if ($end_file_ind == $start_file_ind) { // if this case holds, then outer loop will run only one time.
                $dp_in_range = $end_pos - $start_pos;
            } elseif ($current_ind == $start_file_ind) {
                $dp_in_range = $npoints - $start_pos + 1;
                $end_time = $cur_ind_start_time + ($npoints - 1) * $meta->interval[$layer];
            } elseif ($current_ind == $end_file_ind) {
                $dp_in_range = $end_pos - 1;
                $end_time = $end;
            } else { // load all partition data.
                $dp_in_range = $npoints;
                $end_time = $cur_ind_start_time + ($npoints - 1) * $meta->interval[$layer];
            }

            // 3) Load data values available in time range
            if ($data_exist && $dp_in_range > 0) {
                fseek($fh, ($first_point_pos + 1) * 4);
                $layer_values = unpack("f*",fread($fh, 4*$dp_in_range));
                fclose($fh);
            } elseif ($skipmissing == 1) {
                $current_ind += $meta->interval[$layer];
                $first_point_pos = 0;
                $time_basis = $current_ind * self::WEEK_NPOINTS;
                continue;
            } else { //$skipmissing == 0.
                $layer_values = array();
            }

            $i=0;
            $time0 = 0;
            while($time0 <= $end_time) {
                $time0 = $time_basis + ($outinterval * $i);
                $time1 = $time_basis + ($outinterval * ($i+1));
                $pos0 = round(($time0 - $cur_ind_start_time) / $meta->interval[$layer]);
                $pos1 = round(($time1 - $cur_ind_start_time) / $meta->interval[$layer]);

                $value = null;

                if ($pos0>=0) {
                    $p = $pos0 - $first_point_pos;
                    $point_sum = 0;
                    $points_in_sum = 0;

                    while($p < $pos1 - $first_point_pos) {
                        if (isset($layer_values[$p+1]) && !is_nan($layer_values[$p+1])) {
                            $point_sum += $layer_values[$p+1];
                            $points_in_sum++;
                        }
                        $p++;
                    }

                    if ($points_in_sum) {
                        $value = $point_sum / $points_in_sum;
                    }
                }

                if ($value!==null || $skipmissing===0) {
                    $data[] = array($time0*1000,$value);
                }

                $i++;
            }

            $time_basis = $time0;
            $current_ind += $meta->interval[$layer];
            $first_point_pos = 0;
            if (isset($fh) && is_resource($fh)) fclose($fh);
        }

        $this->log->info("get_data: number of data points returned by get_data function: " . count($data));
        return $data;
    }


    /**
     * Get the last value from a feed
     *
     * @param integer $feedid The id of the feed
    */
    public function lastvalue($id)
    {
        $file_path = $this->dir . "$id" . "/";
        if ( !is_dir($file_path) ) return false;
        if (!$meta = $this->get_meta($id)) return false;

        $file_ind = $this->getFileInd($id, 0);
        if ($file_ind < 0) return array('time'=>0, 'value'=>0);

        $fh = fopen($file_path."0_$file_ind", 'rb');
        $tmp = unpack("I", fread($fh, 4));
        $npoints = $tmp[1];

        if ($npoints > 0) {
            fseek($fh, 4 * $npoints);
            $d = fread($fh, 4);
            fclose($fh);

            $val = unpack("f",$d);
            $time = $file_ind * self::WEEK_NPOINTS + $meta->interval[0] * ($npoints - 1);
            return array('time'=>$time, 'value'=>$val[1]);
        } else {
            return array('time'=>0, 'value'=>0);
        }

    }

    public function export($id,$start,$layer)
    {
        $id = (int) $id;
        $start = (int) $start;
        $layer = (int) $layer;

        // If meta data file does not exist then exit
        if (!$meta = $this->get_meta($id)) {
            $this->log->warn("PHPFiwa partitions:export failed to fetch meta id=$id");
            return false;
        }

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

        $last_file_ind = $this->getFileInd($id, $layer);
        if ($last_file_ind < 0) { //there is no any data posted in the feed yet.
            fclose($fh);
            exit;
        }
        $cur_ind = $this->getFileInd($id, $layer, 0);
        $localsize = intval($start / 4) * 4;

        if ($localsize<4) $localsize = 4;

        while ($current_ind <= $last_file_ind) {
            $file_name = $layer . "_" . $current_ind;
            $file_path = $this->dir . $meta->id . "/" . $file_name;
            $primary = fopen($file_path, 'rb');

            $tmp = unpack("I", fread($fh, 4));
            $primarysize = 4*$tmp[1];

            fseek($primary,$localsize);
            $left_to_read = $primarysize - $localsize;
            if ($left_to_read>0) {
                do {
                    if ($left_to_read > 8192) $readsize = 8192; else $readsize = $left_to_read;
                    $left_to_read -= $readsize;

                    $data = fread($primary,$readsize);
                    fwrite($fh,$data);
                } while ($left_to_read>0);
            }


            $current_ind += $meta->interval[$layer];
            $localsize = 4;
            fclose($primary);
        }
        fclose($fh);
        exit;
    }

    public function delete($id)
    {
        $dir_path = $this->dir . $id . "/";
        if ( !is_dir($dir_path) ) return false;

        $iterator = new DirectoryIterator($dir_path);
        foreach ($iterator as $fileinfo) {
            unlink($dir_path . $fileinfo->getFilename());
        }
        rmdir($dir_path);
    }

    public function get_feed_size($id)
    {
        $dir_path = $this->dir . "$id" . "/";
        if ( !is_dir($dir_path) ) return false;
        if (!$meta = $this->get_meta($id)) return false;

        $size = 0;
        $size += filesize($dir_path . "$id.meta");
        for ($i=0; $i < $meta->nlayers; $i++) {
            $size += 4 * $meta->npoints[$i];
        }
        return $size;
    }

    public function get_meta($id)
    {
        $id = (int) $id;
        $file_path = $this->dir . "$id" . "/$id.meta";

        if (!file_exists($file_path)) {
            return false;
        }

        $meta = new stdClass();
        $meta->id = $id;

        $metafile = fopen($file_path, 'rb');

        $tmp = unpack("I",fread($metafile,4));
        $tmp = unpack("I",fread($metafile,4));
        $meta->nlayers = $tmp[1];

        if ($meta->nlayers<1 || $meta->nlayers>4) {
            $this->log->warn("PHPFiwa partitions:get_meta feed:$id nlayers out of range");
            return false;
        }

        $meta->npoints = array();
        for ($i=0; $i<$meta->nlayers; $i++)
        {
          $tmp = unpack("I",fread($metafile,4));
          $meta->npoints[$i] = $tmp[1];
        }

        $meta->interval = array();
        for ($i=0; $i<$meta->nlayers; $i++)
        {
          $tmp = unpack("I",fread($metafile,4));
          $meta->interval[$i] = $tmp[1];
        }

        fclose($metafile);

        return $meta;
    }

    public function create_meta($id,$meta)
    {
        $id = (int) $id;
        $feed_dir = $this->dir . $id;

        //create a folder for the feed with given feed id, if it does not exist.
        if( !file_exists($feed_dir) ) {
            $this->log->info("feed dir. path->" . $feed_dir);
            mkdir($feed_dir);
        }

        $metafile = fopen($feed_dir . "/" . "$id.meta", 'wb');

        if (!$metafile) {
            $this->log->warn("PHPFIWA partitions:create_meta could not open meta data file id=".$meta->id);
            return false;
        }

        if (!flock($metafile, LOCK_EX)) {
            $this->log->warn("PHPFiwa partitions:create_meta meta file id=".$meta->id." is locked by another process");
            fclose($metafile);
            return false;
        }

        fwrite($metafile,pack("I",$meta->id));
        fwrite($metafile,pack("I",$meta->nlayers));
        foreach ($meta->npoints as $n) fwrite($metafile,pack("I",$n));       // Legacy
        foreach ($meta->interval as $d) fwrite($metafile,pack("I",$d));

        fclose($metafile);
    }

    private function write_padding($fh,$npoints,$npadding)
    {
        $tsdb_max_padding_block = 1024 * 1024;

        // Padding amount too large
        if ($npadding>$tsdb_max_padding_block*2) {
            return false;
        }

        // Maximum points per block
        $pointsperblock = $tsdb_max_padding_block / 4; // 262144

        // If needed is less than max set to padding needed:
        if ($npadding < $pointsperblock) $pointsperblock = $npadding;

        // Fill padding buffer
        $buf = '';
        for ($n = 0; $n < $pointsperblock; $n++) {
            $buf .= pack("f",NAN);
        }

        fseek($fh,4*($npoints+1));

        do {
            if ($npadding < $pointsperblock)
            {
                $pointsperblock = $npadding;
                $buf = '';
                for ($n = 0; $n < $pointsperblock; $n++) {
                    $buf .= pack("f",NAN);
                }
            }

            fwrite($fh, $buf);
            $npadding -= $pointsperblock;
        } while ($npadding);
    }

    public function recompile($meta)
    {

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

        if ($end < $start) return false;
        // If meta data file does not exist then exit
        if (!$meta = $this->get_meta($feedid)) return false;

        if ($outinterval<$meta->interval[0]) $outinterval = $meta->interval[0];

        $layer = 0;
        if ($meta->nlayers>1 && $outinterval >= $meta->interval[1]) $layer = 1;
        if ($meta->nlayers>2 && $outinterval >= $meta->interval[2]) $layer = 2;
        if ($meta->nlayers>3 && $outinterval >= $meta->interval[3]) $layer = 3;

        // 2) find start and end partition, where iteration will take place.
        $denominator = self::WEEK_NPOINTS * $meta->interval[$layer];
        $start_file_ind = floor( $start / $denominator ) * $meta->interval[$layer];
        $end_file_ind = floor( $end / $denominator ) * $meta->interval[$layer];
        $start_pos = 1 + ceil( ($start - $start_file_ind * self::WEEK_NPOINTS) / $meta->interval[$layer] );
        $end_pos = 1 + ceil( ($end - $end_file_ind * self::WEEK_NPOINTS) / $meta->interval[$layer] );

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

        $data = array();
        $first_point_pos = $start_pos;
        $current_ind = $start_file_ind;
        $dp_in_range = 0;
        $first_point_pos = $start_pos - 1;
        $time_basis = $start;
        $end_time = $end;

        while ($current_ind <= $end_file_ind) {
            $file_path = $this->dir . $meta->id . "/" . $layer . "_" . $current_ind;
            $cur_ind_start_time = $current_ind * self::WEEK_NPOINTS;

            if (!file_exists($file_path) || filesize($file_path) <= 4) {
                $current_ind += $meta->interval[$layer];
                $first_point_pos = 0;
                $time_basis = $current_ind * self::WEEK_NPOINTS;
                continue;
            }

            $fh = fopen($file_path, 'rb');
            $tmp = unpack("I", fread($fh, 4));
            $npoints = $tmp[1];

            if ($end_file_ind == $start_file_ind) { // if this case holds, then outer loop will run only one time.
                $dp_in_range = $end_pos - $start_pos;
            } elseif ($current_ind == $start_file_ind) {
                $dp_in_range = $npoints - $start_pos + 1;
                $end_time = $cur_ind_start_time + ($npoints - 1) * $meta->interval[$layer];
            } elseif ($current_ind == $end_file_ind) {
                $dp_in_range = $end_pos - 1;
                $end_time = $end;
            } else { // load all partition data.
                $dp_in_range = $npoints;
                $end_time = $cur_ind_start_time + ($npoints - 1) * $meta->interval[$layer];
            }

            if ($dp_in_range > 0) {
                fseek($fh, ($first_point_pos + 1) * 4);
                $layer_values = unpack("f*",fread($fh, 4*$dp_in_range));
                fclose($fh);
            } else {
                $current_ind += $meta->interval[$layer];
                $first_point_pos = 0;
                $time_basis = $current_ind * self::WEEK_NPOINTS;
                continue;
            }

            $i=0;
            $time0 = 0;
            while($time0 <= $end_time) {
                $time0 = $time_basis + ($outinterval * $i);
                $time1 = $time_basis + ($outinterval * ($i+1));
                $pos0 = round(($time0 - $cur_ind_start_time) / $meta->interval[$layer]);
                $pos1 = round(($time1 - $cur_ind_start_time) / $meta->interval[$layer]);

                $value = null;

                if ($pos0>=0) {
                    $p = $pos0 - $first_point_pos;
                    $point_sum = 0;
                    $points_in_sum = 0;

                    while($p < $pos1 - $first_point_pos) {
                        if (isset($layer_values[$p+1]) && !is_nan($layer_values[$p+1])) {
                            $point_sum += $layer_values[$p+1];
                            $points_in_sum++;
                        }
                        $p++;
                    }

                    if ($points_in_sum) {
                        $value = $point_sum / $points_in_sum;
                        $timenew = $helperclass->getTimeZoneFormated($time0,$usertimezone);
                        fwrite($exportfh, $timenew.$csv_field_separator.number_format($value,$csv_decimal_places,$csv_decimal_place_separator,'')."\n");
                    }
                }

                $i++;
            }

            $time_basis = $time0;
            $current_ind += $meta->interval[$layer];
            $first_point_pos = 0;
            if (is_resource($fh)) fclose($fh);

        }

        fclose($exportfh);
        exit;
    }

    /*
     * assert: there is an existing feed with the given id.
     * assert: layer can be {0, 1, 2, 3}
     * @returns '-1' if there is no any data posted in the feed.
     * @param $t when '!=0', the greatest file index is returned, otherwise the smallest file
     * index
     */
    private function getFileInd($id, $layer, $t=1) {

        $feed_dir_path = $this->dir . "$id" . "/";
        $iterator = new DirectoryIterator($feed_dir_path);
        $arr = array();
        $reg_exp = "/^" . $layer . "_/";
        foreach ($iterator as $fileinfo) {
            if (preg_match($reg_exp, $fileinfo->getFilename())) {
                $arr[] = intval(mb_substr($fileinfo->getFilename(), 2));
            }
        }

        $file_ind = -1;

        if (count($arr) > 0) {
            $file_ind = $t ? $arr[count($arr)-1] : $arr[0];
        }

        return $file_ind;
    }

}
