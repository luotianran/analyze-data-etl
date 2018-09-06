package com.mime.bdp.etl.common.utils

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

object HBaseUtils {

    val conf: Configuration = HBaseConfiguration.create

    val connection: Connection = ConnectionFactory.createConnection(conf)

    // 默认columnFamily
    val DEFAULT_COLUMN_FAMILY: String = "properties"


    /**
      *
      * @param admin
      * @param namespace
      */
    def createNamespaceIfNotExist(admin: Admin, namespace: String): Unit = {
        if (!admin.listNamespaceDescriptors().exists(_.getName == namespace.toLowerCase)) {
            admin.createNamespace(NamespaceDescriptor.create(namespace.toLowerCase).build())
        }
    }

    /**
      *
      * @param namespace
      */
    def createNamespaceIfNotExist(namespace: String): Unit = {
        val admin = connection.getAdmin
        createNamespaceIfNotExist(admin, namespace)
    }

    /**
      * 如果表不存在创建表
      *
      * @param namespace
      * @param tableName
      * @param columnFamily
      */
    def createTableIfNotExist(namespace: String, tableName: String, columnFamily: String): Unit = {
        val admin = connection.getAdmin
        createTableIfNotExist(admin, namespace, tableName, columnFamily)
    }

    /**
      * 如果表不存在创建表
      *
      * @param admin
      * @param namespace
      * @param tableName
      * @param columnFamily
      */
    def createTableIfNotExist(admin: Admin, namespace: String, tableName: String, columnFamily: String): Unit = {
        if (!admin.tableExists(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))) {
            val columnFamilyArray = if (StringUtils.isBlank(columnFamily)) Array(DEFAULT_COLUMN_FAMILY) else columnFamily.split(",")
            val tableDesc = new HTableDescriptor(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
            columnFamilyArray.foreach(columnFamily => tableDesc.addFamily(new HColumnDescriptor(columnFamily.toLowerCase)))
            admin.createTable(tableDesc)
        }
    }

    /**
      * 构建PUT数据记录
      *
      * @param row
      * @param columnFamily
      * @param dataMap
      * @return
      */
    def buildPut(row: Array[Byte], columnFamily: String, dataMap: Map[String, Array[Byte]]): Put = {
        val put = new Put(row)
        dataMap.foreach {
            case (qualifier, data) =>
                put.addColumn(
                    Bytes.toBytes(if (StringUtils.isBlank(columnFamily)) DEFAULT_COLUMN_FAMILY else columnFamily),
                    Bytes.toBytes(qualifier.toLowerCase),
                    data
                )
        }
        put
    }


    /**
      * 存入数据
      *
      * @param tableName
      * @param row
      * @param columnFamily
      * @param dataMap
      */
    def put(namespace: String, tableName: String, row: Array[Byte], columnFamily: String, dataMap: Map[String, Array[Byte]]): Unit = {
        val table = connection.getTable(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
        val put = buildPut(row, columnFamily, dataMap)
        table.put(put)
    }

    /**
      * 批量存入数据
      *
      * @param tableName
      * @param columnFamily
      * @param dataMaps
      */
    def putBatch(namespace: String, tableName: String, columnFamily: String, dataMaps: Iterable[(Array[Byte], Map[String, Array[Byte]])]): Unit = {
        val table = connection.getTable(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
        val puts = dataMaps.map {
            case (rowKey, dataMap) =>
                buildPut(rowKey, columnFamily, dataMap)
        }.toList.asJava
        table.put(puts)
    }


    /**
      * 构建Delete
      *
      * @param rowKey
      * @return
      */
    def buildDelete(rowKey: Array[Byte]): Delete = {
        new Delete(rowKey)
    }

    /**
      * 删除一条
      *
      * @param tableName
      * @param rowKey
      */
    def delete(namespace: String, tableName: String, rowKey: Array[Byte]): Unit = {
        val table = connection.getTable(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
        table.delete(buildDelete(rowKey))
    }

    /**
      * 批量删除
      *
      * @param tableName
      * @param rowKeys
      */
    def deleteBatch(namespace: String, tableName: String, rowKeys: Iterable[Array[Byte]]): Unit = {
        val table = connection.getTable(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
        table.delete(rowKeys.map(buildDelete).toList.asJava)

    }

    /**
      *
      * @param tableName
      * @param startRow
      * @param stopRow
      * @return
      */
    def scan(namespace: String, tableName: String, startRow: Array[Byte], stopRow: Array[Byte]): ResultScanner = {
        val table = connection.getTable(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
        val scan = new Scan(startRow, stopRow)
        table.getScanner(scan)
    }

    /**
      *
      * @param tableName
      * @return
      */
    def scan(namespace: String, tableName: String): ResultScanner = {
        val table = connection.getTable(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
        val scan = new Scan()
        table.getScanner(scan)
    }

    /**
      *
      * @param tableName
      * @param rowKey
      * @return
      */
    def get(namespace: String, tableName: String, rowKey: Array[Byte]): Result = {
        val table = connection.getTable(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
        table.get(new Get(rowKey))
    }

    /**
      *
      * @param tableName
      * @param rowKeys
      * @return
      */
    def get(namespace: String, tableName: String, rowKeys: Iterable[Array[Byte]]): Array[Result] = {
        val table = connection.getTable(TableName.valueOf(namespace.toLowerCase, tableName.toLowerCase))
        val gets = rowKeys.map(new Get(_)).toList.asJava
        table.get(gets)
    }


}
