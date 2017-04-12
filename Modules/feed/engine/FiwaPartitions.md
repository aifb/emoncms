# FiwaPartitions

FiwaPartitions is a storage engine, which extends Fiwa. It is intended to be used in cases, where a huge amount of data needs to be handled and big gaps in data stream can occur. The main difference to Fiwa is that the data of a single feed is stored over multiple partitions, while Fiwa stores the whole data in a single file. Here, partition refers to a data file, which contains a certain *contiguous* part of the data. In the current implementation, each partition can contain at most 604800 data points, that is, *one week* data, given that the time interval is *1s*. The main advantage is that the potential big gaps are avoided, while in Fiwa they are filled with NaN values, and thereby, wasting the storage.


The single data points are stored in the same format as in Fiwa, that is, without the timestamp. In the Section **Deriving Timestamp of Data Point** we explain how the timestamp of a data point can be retrieved, from the metadata of the partition (partition ID and layer interval) containing that data point and relative position of the data point within that partition.


## Directory Structure

There is a root directory called *fiwaPartitions*, located in `/var/lib` (in linux OS), which hosts all the data associated with the engine FiwaPartitions. For each feed using this engine, there is a corresponding subdirectory in the root directory, named with the id of that feed. Here all the data associated with that feed is stored.

## Week Index

In this context, week index is defined with respect to the unixtime. In the way that the unixtime is counting number of seconds that have elapsed since epoch (=1. January 1970, 00:00), the week index is counting the number of  weeks that have elapsed since epoch. E.g. `week index = 0` spans all unixtimes appearing in the period of from *1st January 1970 00:00* untill *8th January 1970 00:00*; the `week index = 1` spans all unixtimes appearing in the period of from *8th January 1970 00:00* till *15th January 1970 00:00*, etc. With that in mind, each week index spans an interval of unixtimes, `[first_unixtime_in_that_week, last_unixtime_in_that_week]`, of size 604800 (=number of seconds within one week). Given an unix timestamp, the week index can be derived by computing

   week index = floor( timestamp / 604800 )

## Number of Weeks that a single Partition can span

As we have said at the beginning, currently partition spans at most 604800 data points

    partition size = 604800 data points
    
The number of data points published within one week is dependent on the interval size of the layer. Given that the `time interval >= 1s`

    #data points within one week = 604800 / interval

Now, given the maximum number of data points that one week can contain and partition size, we can compute the maximum number of weeks that a single partition can span:

    #weeks in one partition = partition size / #data points within one week
                            = interval
                            
## Naming Convention for Partitions

Each partition is a data file, which has the name of the form: `<layer_ind>_<partition_ID>`, e.g. `1_10` => `layer_ind = 1` and `partition_ID = 10`. Here, partition_ID is not an arbitrary number assigned to the partition, but a *week index* which defines the beginning of that partition. The first timestamp in that partition is equal to the first timestamp within that week of index. Thus, for the partition given in the above example, the timestamp of the first data location is computed as follows:

    timestamp of first location = partition_ID * 604800
                                = 10 * 604800 = 6048000

## Accessing Data Point

To access any data point we need to know in *which partition* it is stored and what is the *relative position* of that data point within its partition. In the following, we discuss, how the *partition ID* and *relative position* can be computed.

### Deriving Partition ID

*Question*: Given a timestamp and the time interval of the considered layer, how to derive the partition ID where a data point with the given timestamp is to be stored?

Firstly, we determine the week index of the given timestamp, to which it belongs to.

    week index = floor( timestamp / 604800 )

Now, we find out, in which partition the timestamps of the week of that index are contained. We keep in mind that the partition id is the index of the first week included in that partition.

    Partition ID = floor( week index / interval ) * interval

Put together, we have

    Partition ID = floor( timestamp / (604800 * interval) ) * interval
    
### Deriving Relative Position

*Question*: Given a timestamp and the time interval of the considered layer, how to derive the relative position of a data point with the given timestamp within the corresponding partition?

Firstly, the given timestamp must be rounded (normalized)  in order to localize it within the timeseries of the fixed interval.

    timestamp = floor( timestamp / interval ) * interval
    
Then, we compute the first timestamp of a partition, which contains the given timestamp.

    Partition ID = floor( timestamp /  (604800 * interval) ) * interval 

First timestamp of the partition is the first timestamp within the week with `week index = Partition ID`. Therefore, to compute the first timestamp within a partition, we compute the first timestamp within the corresponding week:

    first_timestamp = week index * 604800 
                    = Partition ID * 604800

In the current implementation, the first location of the partition is reserved for storing the *current number of data points* in that partition. Therefore, the numbering of the memory locations of data points start with `1` instead of with `0`. Having said that, the relative position of a data point with the given timestamp is computed as follows:

    relative position = 1 + (timestamp - first_timestamp) / interval 
    
## Deriving Timestamp of Data Point

*Question*: How to compute the timestamp of a data point, when its partition (partition ID and layer interval) and the relative position within that partition is known?

Firstly, we derive the timestamp of the first data point location within that partition

    timestamp of first location = partition ID * 604800
    
The timestamp of the data point is computed as follows:

    timestamp = timestamp of first location + (relative position - 1) * interval

Keep in mind that the first location within a partition contains the current number of points within that partition. Therefore, we subtract 1 from relative position.
