package com.mime.bdp.etl.common.utils

import java.util
import java.util.Properties

import org.apache.commons.configuration.{Configuration, PropertiesConfiguration}
import org.apache.commons.dbcp2.{BasicDataSource, BasicDataSourceFactory}
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.MapHandler
import org.apache.commons.lang.StringUtils

object DBUtils {

    val dbConfig: Configuration = new PropertiesConfiguration("database.properties")

    val properties: Properties = {
        val properties = new Properties()
        properties.put("driverClassName", dbConfig.getString("mysql.driver"))
        properties.put("url", dbConfig.getString("mysql.url"))
        properties.put("username", dbConfig.getString("mysql.user"))
        properties.put("password", dbConfig.getString("mysql.password"))
        properties
    }

    val dataSource: BasicDataSource = BasicDataSourceFactory.createDataSource(properties)

    val queryRunner: QueryRunner = new QueryRunner(dataSource)


    def queryCount(dbName: String, tableName: String, where: String): Long = {
        var sql = s"select count(1) as count from $dbName.$tableName"
        if (StringUtils.isNotBlank(where)) {
            sql += s" where $where"
        }
        val dataMap: util.Map[String, Object] = queryRunner.query(sql, new MapHandler())
        dataMap.get("count").asInstanceOf[Long]
    }


}
