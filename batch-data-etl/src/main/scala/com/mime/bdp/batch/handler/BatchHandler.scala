package com.mime.bdp.batch.handler

import java.sql.{Date, Timestamp}
import java.util.Properties

import com.mime.bdp.batch.utils.SparkSqlUtils
import com.mime.bdp.etl.common.utils.HBaseUtils
import org.apache.commons.configuration.{Configuration, PropertiesConfiguration}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class BatchHandler extends Serializable {


    def start(): Unit = {

        val spark: SparkSession = SparkSession.builder.appName("batch-data-etl").master("local[1]").getOrCreate()

        // 加载所有表的表元数据信息
        val tableMetasDF = loadTableMetasBroadcast(spark)
        //按照数据库和表名分组，遍历，加载mysql数据，存入HBase
        val tableArray = tableMetasDF.rdd.groupBy(tableMeta => {
            tableMeta.getAs[String]("TABLE_SCHEMA")
        }).flatMap {
            case (dbName, iterable) =>
                //如果Namespace不存在，创建
                HBaseUtils.createNamespaceIfNotExist(dbName)
                iterable.map(row => (dbName, row.getAs[String]("TABLE_NAME")) -> (row.getAs[String]("COLUMN_NAME"), row.getAs[String]("DATA_TYPE")))
        }.groupByKey(3).collect()

        tableArray.foreach {
            case ((dbName, tableName), iterable) =>
                //如果HBase表不存在则创建
                HBaseUtils.createTableIfNotExist(dbName, tableName, null)
                //从mysql表中加载数据
                val tableDF = SparkSqlUtils.loadFromMysql(spark, dbName, tableName,
                    getPartitionColumn(iterable))
                //将row转换为put
                tableDF.rdd.map(row => {
                    //rowKey默认为mysql主键，如有联合主键，相加，没有主键，则将所有列相加生成rowKey
                    var rowKey = Array[Byte]()
                    val dataMap: Map[String, Array[Byte]] = row.schema.map(field => {
                        if (iterable.isEmpty) {
                            rowKey ++= convertFiled(field, row)
                        } else {
                            if (iterable.exists(_._1 == field.name)) {
                                if (row.getAs(field.name) != null) {
                                    rowKey ++= convertFiled(field, row)
                                }
                            }
                        }
                        field.name.toLowerCase -> convertFiled(field, row)
                    }).toMap
                    //构建HBase Put
                    (new ImmutableBytesWritable, HBaseUtils.buildPut(rowKey, null, dataMap))
                }).saveAsHadoopDataset(getJobConf(dbName, tableName)) //将数据保存入HBase File文件中

        }
    }


    private def getPartitionColumn(iterable: Iterable[(String, String)]): String = {
        if (iterable.nonEmpty) {
            val col = iterable.take(1).last
            if (col._2 == "bigint" || col._2 == "int") {
                col._1
            } else null
        } else null
    }


    /**
      * 构建Hadoop JobConf
      *
      * @param tableName
      * @return
      */
    private def getJobConf(dbName: String, tableName: String): JobConf = {
        val jobConf = new JobConf(HBaseUtils.conf)
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, s"$dbName:$tableName")
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf
    }

    /**
      * 将row中的field.value转换为Array[Byte]
      *
      * @param field
      * @param row
      * @return
      */
    def convertFiled(field: StructField, row: Row): Array[Byte] = {
        if (row.getAs(field.name) == null) {
            null
        } else {
            field.dataType match {
                case StringType =>
                    Bytes.toBytes(row.getAs[String](field.name))
                case IntegerType =>
                    Bytes.toBytes(row.getAs[Int](field.name))
                case LongType =>
                    Bytes.toBytes(row.getAs[Long](field.name))
                case TimestampType =>
                    Bytes.toBytes(row.getAs[Timestamp](field.name).toString)
                case DecimalType() =>
                    Bytes.toBytes(row.getAs[java.math.BigDecimal](field.name))
                case NullType => null
                case DoubleType =>
                    Bytes.toBytes(row.getAs[Double](field.name))
                case BooleanType =>
                    Bytes.toBytes(row.getAs[Boolean](field.name))
                case DateType =>
                    Bytes.toBytes(row.getAs[Date](field.name).toString)
                case ByteType =>
                    Bytes.toBytes(row.getAs[Byte](field.name))
                case FloatType =>
                    Bytes.toBytes(row.getAs[Float](field.name))
                case CharType(1) =>
                    Bytes.toBytes(row.getAs[Char](field.name))
                case _ =>
                    Bytes.toBytes(row.getAs(field.name).toString)
            }
        }
    }

    /**
      * 加载表元数据信息，包含数据库名，表名，主键
      *
      * @param spark
      * @return
      */
    def loadTableMetasBroadcast(spark: SparkSession): DataFrame = {
        val tableMetasDF = SparkSqlUtils.loadTableMetas(spark)
        val columnMetaBC = spark.sparkContext.broadcast(SparkSqlUtils.loadColumnMetas(spark))
        tableMetasDF.join(columnMetaBC.value, Seq("TABLE_SCHEMA", "TABLE_NAME"), "left_outer")
    }

}
