package com.mime.bdp

import java.sql.{Date, Timestamp}

import com.mime.bdp.batch.utils.SparkSqlUtils
import com.mime.bdp.etl.common.utils.{DBUtils, HBaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.junit._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

@Test
class AppTest {

    val spark: SparkSession = SparkSession.builder.appName("batch-data-etl").master("local[1]").getOrCreate()

    @Test
    def test1(): Unit = {
        SparkSqlUtils.loadTableMetas(spark).first().schema.foreach(field => println(field))
        val a = SparkSqlUtils.loadTableMetas(spark).first().getAs[Timestamp]("CREATE_TIME").toString.getBytes
        println(a)
    }

    @Test
    def test2(): Unit = {
        val df = SparkSqlUtils.loadFromMysql(spark, "credit_audit", "ca_bur_operator_contact_recent", "")
        //df.show()
        df.foreach(row => {
            row.schema.foreach(f => {
                //println(s"${f.name}, ${row.getAs(f.name)}, ${App.convertFiled(row, f)}")
            })
        })
        /*val row = df.first()
        row.schema.foreach(f => {
            println(s"$f, ${row.getAs(f.name)}, ${convertFiled(row, f)}")
        })*/

    }

    @Test
    def test3(): Unit = {
        val tableMetasDF = SparkSqlUtils.loadTableMetas(spark)
        val columnMetaBC = spark.sparkContext.broadcast(SparkSqlUtils.loadColumnMetas(spark))
        tableMetasDF.join(columnMetaBC.value, Seq("TABLE_SCHEMA", "TABLE_NAME"), "left_outer").show()
    }

    @Test
    def test4(): Unit = {
        SparkSqlUtils.loadFromMysql(spark, "credit_audit", "ca_bur_operator_contact_recent", "").show()
    }

    private val jobConf = {
        val jobConf = new JobConf(HBaseConfiguration.create())
        jobConf.set("hbase.zookeeper.quorum", "10.81.55.29")
        //jobConf.set("zookeeper.znode.parent", "/hbase")
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "testdb:testtable")
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf
    }

    @Test
    def test5(): Unit = {
        val rdd = spark.sparkContext.makeRDD(Array(1)).flatMap(_ => 0 to 1000)
        rdd.map(x => {
            val put = new Put(Bytes.toBytes(x.toString))
            put.addColumn(Bytes.toBytes(HBaseUtils.DEFAULT_COLUMN_FAMILY), Bytes.toBytes("c1"), Bytes.toBytes(x.toString))
            (new ImmutableBytesWritable, put)
        }).saveAsHadoopDataset(jobConf)
    }

    @Test
    def test6(): Unit = {
        val count = DBUtils.queryCount("authdb", "AUTH_DEVICE", "")
        println(count)
    }


}


