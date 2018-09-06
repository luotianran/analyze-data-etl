package com.mime.bdp

import java.util.Properties

import org.junit._
import Assert._
import com.mime.bdp.etl.common.utils.HBaseUtils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.{NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

@Test
class AppTest {

    @Test
    def testOK(): Unit = {
        /*val a = "ab"
        val b = "bc"
        Bytes.toBytes(a + b).foreach(println)
        println()
        (Bytes.toBytes(a) ++ Bytes.toBytes(b)).foreach(println)
        println(Bytes.toBytes(a + b) == (Bytes.toBytes(a) ++ Bytes.toBytes(b)))*/

        println(Math.ceil(3.2).asInstanceOf[Long])

        val iterable = Iterable(1, 2)
        println(iterable.take(1).last)
        iterable.foreach(println)

        val props = new Properties()


        props.put("numPartitions", Math.ceil(3.2).asInstanceOf[Long]: java.lang.Long)


    }


    @Test
    def test1(): Unit = {
        val result = HBaseUtils.scan("testdb1", "test_table", Bytes.toBytes("1"), Bytes.toBytes("2"))
        result.asScala.foreach(row => println(new String(row.getRow)))

    }

    @Test
    def test2(): Unit = {
        val result = HBaseUtils.scan("testdb1", "test_table")
        result.asScala.foreach(row => println(new String(row.getRow)))
    }

    @Test
    def test3(): Unit = {
        HBaseUtils.createTableIfNotExist("testdb1", "testtable", null)
    }

    @Test
    def test4(): Unit = {
        val admin = HBaseUtils.connection.getAdmin
        admin.listNamespaceDescriptors().foreach(db => println(db.getName))
        /*val namespaceDescriptor = admin.getNamespaceDescriptor("testdb")
        println(namespaceDescriptor == null )*/
        if (!admin.listNamespaceDescriptors().exists(_.getName == "testdb")) {
            admin.createNamespace(NamespaceDescriptor.create("testdb").build())
        }
        println("*******************")
        admin.listNamespaceDescriptors().foreach(db => println(db.getName))
    }

    @Test
    def test5(): Unit = {
        val admin = HBaseUtils.connection.getAdmin
        admin.listTableDescriptorsByNamespace("authdb").foreach(t => println(t.getNameAsString))
    }

    @Test
    def test6(): Unit = {
        val table = HBaseUtils.connection.getTable(TableName.valueOf("testdb2", "testtable"))
        println(table.getName.getNameAsString)
    }

    @Test
    def test7(): Unit = {
        println(HBaseUtils.connection.getAdmin.tableExists(TableName.valueOf("credit_audit", "ca_bur_operator_contact_recent")))
    }

    @Test
    def test8(): Unit = {
        val scan = HBaseUtils.scan("credit_audit", "ca_bur_operator_contact_recent")
        println("*********************")
        scan.asScala.foreach(result => {
            val dataMap = result.getFamilyMap(Bytes.toBytes(HBaseUtils.DEFAULT_COLUMN_FAMILY)).asScala.map(x => new String(x._1) -> new String(x._2))
            println(s"rowkey:${Bytes.toLong(result.getRow)}, $dataMap")
        })
        println("*********************")
    }

    @Test
    def test9(): Unit ={
        val admin = HBaseUtils.connection.getAdmin
        val tableName = TableName.valueOf("credit_audit", "ca_bur_operator_contact_recent")
        println(admin.isTableDisabled(tableName))
        admin.disableTable(tableName)
        admin.truncateTable(tableName, true)
        //admin.enableTable(tableName)
    }

}


