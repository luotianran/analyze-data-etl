package com.mime.bdp.real.time.handler

import com.mime.bdp.etl.common.utils.HBaseUtils
import com.mime.bdp.real.time.RealTimeMain
import com.mime.bdp.real.time.utils.{AvroSerializerUtils, DTSHandlerUtils}
import com.mime.dbsync.model.DTSRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._


class RealTimeHandler extends Serializable {


    /**
      * 初始化Kafka参数
      */
    val kafkaParams: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "10.27.239.62:9092,10.27.239.119:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[ByteArrayDeserializer],
        "group.id" -> "real-time-analyze-data-etl",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /**
      * 构建KafkaDirectStream
      *
      * @return
      */
    def getKafkaDirectStream(ssc: StreamingContext): InputDStream[ConsumerRecord[String, Array[Byte]]] = {
        val topics = Array("com.mime.dts.DTSRecord")
        KafkaUtils.createDirectStream[String, Array[Byte]](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, Array[Byte]](topics, kafkaParams)
        )
    }

    /**
      * 根据recordType类型过滤记录
      *
      * @param recordType
      * @return
      */
    def recordFilterByType(recordType: String): Boolean = {
        RealTimeMain.INSERT == recordType ||
            RealTimeMain.UPDATE == recordType ||
            RealTimeMain.DELETE == recordType
    }


    def start(): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("real-time-data-etl")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
        val inStream = getKafkaDirectStream(ssc)
        inStream.foreachRDD(recordRDD => {
            val offsetRanges = recordRDD.asInstanceOf[HasOffsetRanges].offsetRanges
            recordRDD.map(record => {
                val value = record.value()
                //反序列化Array[Byte]，解析为DTSRecord
                val dtsRecord: DTSRecord = AvroSerializerUtils.dtsRecordDeserializer.deserialize(value)
                dtsRecord
            }).filter(dtsRecord => recordFilterByType(dtsRecord.getRecordType.toString)).filter(_.getTableName.toString.toLowerCase == "ca_bur_operator_contact_recent")
                .map(dtsRecord => {
                    val (dbName, tableName, rowKey, dataMap, op) = DTSHandlerUtils.get(dtsRecord)
                    dbName -> (tableName, op, rowKey, dataMap)
                }).groupByKey()
                .flatMap {
                    case (dbName, iterable) =>
                        HBaseUtils.createNamespaceIfNotExist(dbName)
                        iterable.map {
                            case (tableName, op, rowKey, dataMap) =>
                                (dbName, tableName) -> (rowKey, dataMap, op)
                        }
                }.groupByKey().flatMap {
                case ((dbName, tableName), iterator) =>
                    // 如果表不存在，创建
                    HBaseUtils.createTableIfNotExist(dbName, tableName, null)
                    iterator.map {
                        case (rowKey, dataMap, op) =>
                            (dbName, tableName, op) -> (rowKey, dataMap)
                    }
            }.groupByKey().foreach {
                case ((dbName, tableName, op), opIterator) =>
                    op match {
                        // 写入数据
                        case RealTimeMain.INSERT | RealTimeMain.UPDATE =>
                            HBaseUtils.putBatch(dbName, tableName, null, opIterator)
                        // 删除数据
                        case RealTimeMain.DELETE =>
                            HBaseUtils.deleteBatch(dbName, tableName, opIterator.map(_._1))
                    }
            }
            inStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        })

        ssc.start()
        ssc.awaitTermination()
    }

}
