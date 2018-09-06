package com.mime.bdp.real.time.utils

import com.mime.bdp.real.time.RealTimeMain
import com.mime.dbsync.model.{DTSField, DTSRecord}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

object DTSHandlerUtils {


    /**
      * 解析DTSRecord得到dbName，tableName，主键Array[Byte]，属性数据Map[String, Array[Byte]
      * 主键Array[Byte]规则：唯一主键，返回主键Array[Byte]；联合主键，++=返回Array[Byte]；没有主键，所有属性++=返回Array[Byte]
      * @param dTSRecord
      * @return
      */
    def get(dTSRecord: DTSRecord): (String, String, Array[Byte], Map[String, Array[Byte]], String) = {
        var rowKey: Array[Byte] = Array[Byte]()
        val dataMap = collection.mutable.Map[String, Array[Byte]]()
        val primaryColumn = if (dTSRecord.getPrimary == null) null else dTSRecord.getPrimary.toString
        val isUpdate = dTSRecord.getRecordType == RealTimeMain.UPDATE
        dTSRecord.getFields.asScala.zipWithIndex.foreach{
            case (field, index) =>
                if (!isUpdate || index % 2 == 0) {
                    if (StringUtils.isNotBlank(primaryColumn)) {
                        if (field.getIsPrimary) {
                            if (field.getValue != null) {
                                rowKey ++= field.getValue.array()
                            }
                        }
                    } else {
                        rowKey ++= field.getValue.array()
                    }
                    dataMap += (field.getFieldname.toString.toLowerCase -> field.getValue.array())
                }
        }
        (dTSRecord.getDb.toString, dTSRecord.getTableName.toString, rowKey, dataMap.toMap, dTSRecord.getRecordType.toString)
    }





}
