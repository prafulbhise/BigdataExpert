package com.spark.streaming.mq

import java.net.ServerSocket
import java.util.{Properties, UUID}
import javax.jms._
import com.ibm.mq.jms._
import javax.naming.{Context, InitialContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Minutes, Seconds}
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.spark.streaming.jms.JmsStreamUtils
import org.spark.streaming.jms.MessageConsumerFactory
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.concurrent.duration._
import java.util.Date

object MQReceiver {
  def functionToCreateContext(host: String, port: String, qm: String, qn: String, checkpointDirectory: String): StreamingContext = {
        val sparkConf = new SparkConf().setAppName("MQJMSReceiver").set("spark.streaming.receiver.writeAheadLog.enable", "true")
        val sc = new SparkContext(sparkConf)
        
        val ssc = new StreamingContext(sc, Seconds(1))
        val sqlContext = new SQLContext(sc)
        
        ssc.checkpoint(checkpointDirectory)
        
        val user = ""
        val credentials = ""
        println(s"user= $user")
        
        val converter: Message => Option[String] = {
            case msg: TextMessage =>
                Some(msg.getText)
            case _ =>
                None
        }
        
        println(s"MQ Host=$host")
        println(s"MQ Port=$port")
        println(s"MQ Queuename=$qn")
        val msgs = JmsStreamUtils.createSynchronousJmsQueueStream(ssc, MQConsumerFactory(host, port.toInt, qm, qn, user, credentials),
                                                                 converter,
                                                                  1000,
                                                                  10.second,
                                                                  30.seconds,
                                                                  StorageLevel.MEMORY_AND_DISK_SER_2
                                                                 )
        import sqlContext.implicits._
        msgs.foreachRDD ( rdd => {
            if (!rdd.partitions.isEmpty){                
                println("messages received:")
                //rdd.foreach(println)               
               // rdd.saveAsTextFile("hdfs://hanameservice/hdev/tmp/app/mqdata/scv-"+Calendar.getInstance().getTimeInMillis())
                val dataFrame = rdd.toDF()
                val calender = Calendar.getInstance()
                calender.roll(Calendar.DAY_OF_YEAR, 0)
                val sdf = new SimpleDateFormat("yyyy-MM-dd")
                val date = sdf.format(calender.getTime())
                //dataFrame.write.mode("append").format("text").save("adl://<adls_name>.azuredatalakestore.net/datalake-dev/tmp/app_name/mqdata/scv-"+Calendar.getInstance().getTimeInMillis())
                dataFrame.write.mode("append").format("text").save("adl://<adls_name>.azuredatalakestore.net/datalake-dev/tmp/app_name/mqdata/date_part="+date)
                //rdd.saveAsTextFile("adl://<adls_name>.azuredatalakestore.net/datalake-dev/tmp/app_name/mqdata/scv-"+Calendar.getInstance().getTimeInMillis())
            } else {
                println("rdd is empty")
            }
        })
        
        ssc
    }
    
    def main(args: Array[String]): Unit = {
        if (args.length < 4){
            System.err.println("Usage: <host> <port> <queue manager> <queue name>")
            System.exit(1)
        }
        
        val Array(host, port, qm, qn) = args
                
        val checkpointDirectory = "hdfs://hanameservice/hdev/tmp/app/mqcheckpoint"
        
        // fetch StreamingContext from checkpoint dir already created or create a new dir
        val streamC = StreamingContext.getOrCreate(checkpointDirectory, () => functionToCreateContext(host, port, qm, qn, checkpointDirectory))
        val state = streamC.getState()
        println(s"State of Streaming = $state")
        streamC.start()
        streamC.awaitTermination()
    }
}