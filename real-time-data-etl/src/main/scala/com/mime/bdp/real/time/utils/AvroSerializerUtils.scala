package com.mime.bdp.real.time.utils

import java.io.IOException

import com.mime.dbsync.model.DTSRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

object AvroSerializerUtils {

    val dtsRecordDeserializer: AvroDeserializer[DTSRecord] = getDeserializer[DTSRecord]

    def getDeserializer[A <: org.apache.avro.specific.SpecificRecordBase]: AvroDeserializer[A] = {
        new AvroDeserializer[A]
    }

    class AvroDeserializer[A <: org.apache.avro.specific.SpecificRecordBase] {

        /**
          * 初始化SpecificDatumReader
          */
        val reader: SpecificDatumReader[A] = {
            val specificDatumReader = new SpecificDatumReader[A]
            specificDatumReader.setSchema(DTSRecord.getClassSchema)
            specificDatumReader
        }

        /**
          * 反序列化Array[Byte]，解析为SpecificRecordBase或其子类
          * @param msg
          * @return
          */
        @throws[IOException]
        @throws[IllegalStateException]
        def deserialize(msg: Array[Byte]): A = {
            val decoder = DecoderFactory.get.binaryDecoder(msg, null)
            reader.read(null.asInstanceOf[A], decoder)
        }
    }


}
