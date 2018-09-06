package com.mime.bdp

import com.mime.bdp.etl.common.utils.HBaseUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

class SparkHBaseTest {

    val spark: SparkSession = SparkSession.builder.appName("tab merge").master("local").enableHiveSupport().getOrCreate()

    private val conf = HBaseConfiguration.create()

    private val connection = ConnectionFactory.createConnection(conf)

    private val jobConf = {
        val jobConf = new JobConf(conf)
        jobConf.set("hbase.zookeeper.quorum", "10.81.55.29")
        //jobConf.set("zookeeper.znode.parent", "/hbase")
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test_table1")
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf
    }

    @Test
    def test1(): Unit = {
        val rdd = spark.sparkContext.makeRDD(Array(1)).flatMap(_ => 0 to 1000)
        rdd.map(x => {
            val put = new Put(Bytes.toBytes(x.toString))
            put.addColumn(Bytes.toBytes("test_column"), Bytes.toBytes("c1"), Bytes.toBytes(x.toString))
            (new ImmutableBytesWritable, put)
        }).saveAsHadoopDataset(jobConf)
    }

    @Test
    def test2(): Unit = {
        //println(HBaseUtils.get("test_table", "1"))
    }


}
