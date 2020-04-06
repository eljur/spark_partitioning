           
import math
import hashlib

DF1 = spark.read.parquet("..")
DF1.createOrReplaceTempView("log_lines")
DF2 = spark.sql("SELECT advertisable_eid, cookie, timestamp from log_lines where type='pxl'")
DF2.createOrReplaceTempView("pxl_lines_0328")

pxlrdd =DF2.rdd
keypair_pxlrdd = pxlrdd.map(lambda x : (x[0:2],x[2:]))

DF3 =  spark.sql("select advertisable_eid, count(1) as cnt from pxl_lines_0328 group by advertisable_eid")

mapping = {k: (cnt,) for k, cnt in 
           DF3.rdd.map(lambda x: (x[0], x[1])).collect()}

def partition_mapper(count_map, thresh=200000):
    counter = 0
    start = 0
    for k in count_map.keys():
        threshold = thresh
        zu = count_map[k][0]
        cnt = count_map[k][0]
        buckets = 1
    #allow bigger partitions to limit key spread over multiple partitions
        if threshold < counter + cnt < threshold * 1.2:
           threshold = int(threshold * 1.2)
        if counter + cnt > threshold:
            if cnt < threshold:
                buckets = 2
            else:
                buckets = math.ceil(cnt/threshold)
            cnt = cnt - (threshold - counter)
            counter = cnt % threshold
        else:
            counter += cnt
        count_map[k] += (start, buckets)
        start += buckets - 1
    return count_map
    
mapping = partition_mapper(mapping)
overall_partitions = max([i for a, i, k in mapping_copy.values()])

def adv_partitioner(k):
    global mapping
    map_key = k[0]
    hash_key =  k[1]
    start_partition =  mapping_copy[map_key][1]
    total_partitions = mapping_copy[map_key][2]
    distkey = int(hashlib.md5(hash_key.encode('utf-8')).hexdigest(), 16)
    partition_no = distkey % total_partitions
    return start_partition + partition_no
    
newpxl = keypair_pxlrdd.partitionBy(overall_partitions, adv_partitioner)

print("Partitions structure: {}".format(newpxl.glom().map(len).collect()))
