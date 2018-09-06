package com.mime.bdp.batch.utils

import java.util.Properties

import com.mime.bdp.etl.common.utils.DBUtils
import org.apache.commons.configuration.{Configuration, PropertiesConfiguration}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSqlUtils extends Serializable {

    val dbConfig: Configuration = new PropertiesConfiguration("database.properties")

    val COUNT_PER_PARTITION: Long = 100 * 10000

    val commonProps: Properties = {
        val properties = new Properties()
        properties.put("driver", dbConfig.getString("mysql.driver"))
        properties.put("user", dbConfig.getString("mysql.user"))
        properties.put("password", dbConfig.getString("mysql.password"))
        properties
    }


    def getProps(partitionColumn: String, count: Long): Properties = {
        val props = commonProps
        if (StringUtils.isNotBlank(partitionColumn)) {
            props.put("partitionColumn", partitionColumn)
            props.put("lowerBound", "0")
            props.put("upperBound", Long.MaxValue.toString)
        }
        props.put("numPartitions", Math.ceil(count / COUNT_PER_PARTITION).asInstanceOf[Long].toString)
        props
    }

    def loadFromMysql(spark: SparkSession, dbName: String, tableName: String, partitionColumn: String): DataFrame = {
        val count = DBUtils.queryCount(dbName, tableName, null)
        spark.read.jdbc(dbConfig.getString("mysql.url"), s"$dbName.$tableName", getProps(partitionColumn, count))
    }


    def loadFromMysql(spark: SparkSession, dbName: String, tableName: String, where: String, partitionColumn: String): DataFrame = {
        loadFromMysql(spark, dbName, tableName, partitionColumn).where(where)
    }


    def saveToMysql(df: DataFrame, dbName: String, tableName: String, saveMode: SaveMode = SaveMode.Append): Unit = {
        df.write.mode(saveMode).jdbc(dbConfig.getString("mysql.url"), s"$dbName.$tableName", commonProps)
    }


    def loadTableMetas(spark: SparkSession): DataFrame =
        loadFromMysql(spark, "information_schema", "tables",
            "TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA NOT IN ('mysql','performance_schema','datatech_department')" +
                "AND TABLE_NAME = 'ca_bur_operator_contact_recent'", null)


    def loadColumnMetas(spark: SparkSession): DataFrame =
        loadFromMysql(spark, "information_schema", "columns",
            "TABLE_SCHEMA NOT IN ('mysql','performance_schema','information_schema') AND column_key IN ('pri')", null)


}
