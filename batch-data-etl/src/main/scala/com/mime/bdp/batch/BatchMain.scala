package com.mime.bdp.batch

import com.mime.bdp.batch.handler.BatchHandler

object BatchMain {

    def main(args: Array[String]): Unit = {

        try {
            new BatchHandler().start()
        } catch {
            case e: Exception =>
                e.printStackTrace()
        }

    }

}
