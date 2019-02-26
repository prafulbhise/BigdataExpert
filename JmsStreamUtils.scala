package org.spark.streaming.jms

import java.util.Properties
import javax.jms._
import javax.naming.{Context, InitialContext}
import org.spark.streaming.jms.{ PublicLogging => Logging }
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.spark.streaming.jms._
import org.apache.spark.streaming.receiver.{BlockGeneratorListener, Receiver}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.collection.JavaConverters._


object JmsStreamUtils {
  
  println("Inside JMSStreamutils")
  def createSynchronousJmsQueueStream[T: ClassTag](ssc: StreamingContext,
                                                   consumerFactory: MessageConsumerFactory,
                                                   messageConverter: (Message) => Option[T],
                                                   batchSize: Int = 1000,
                                                   maxWait: Duration = 10.second,
                                                   maxBatchAge: Duration = 30.seconds,
                                                   storageLevel: StorageLevel =
                                        StorageLevel.MEMORY_AND_DISK_SER_2
                                       ): ReceiverInputDStream[T] = {

    ssc.receiverStream(new SynchronousJmsReceiver[T](consumerFactory,
      messageConverter,
      batchSize,
      maxWait,
      maxBatchAge,
      storageLevel))

  }

  def createAsynchronousJmsQueueStream[T: ClassTag](ssc: StreamingContext,
                                                    consumerFactory: MessageConsumerFactory,
                                                    messageConverter: (Message) => Option[T],
                                                    acknowledgementMode: Int,
                                                    storageLevel: StorageLevel =
                                                    StorageLevel.MEMORY_AND_DISK_SER_2
                                                   ): ReceiverInputDStream[T] = {

    ssc.receiverStream(new AsynchronousJmsReceiver[T](consumerFactory,
      messageConverter,
      acknowledgementMode,
      storageLevel
    ))

  }
  
}