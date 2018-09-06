package com.mime.bdp

import java.sql.{Date, Timestamp}

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Hello world!
  *
  */
object App {

    def main(args: Array[String]): Unit = {

        /*val a = "1"
        val b = 1
        println(Bytes.toBytes(a).foreach(print(_)))
        println(Bytes.toBytes(b).foreach(print(_)))*/

        val a = 1
        val b = 2
        val array1 = Bytes.toBytes(a) ++ Bytes.toBytes(b)
        Bytes.toBytes(a + b).foreach(print(_))
        println()
        println("**********")
        array1.foreach(print(_))


    }


}
