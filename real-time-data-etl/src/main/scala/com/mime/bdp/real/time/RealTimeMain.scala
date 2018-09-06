package com.mime.bdp.real.time

import com.mime.bdp.real.time.handler.RealTimeHandler

object RealTimeMain {

    val INSERT = "insert"

    val UPDATE = "update"

    val DELETE = "delete"

    def main(args: Array[String]): Unit = {

        try {
            new RealTimeHandler().start()
        } catch {
            case e: Exception => e.printStackTrace()
        }

    }

}
