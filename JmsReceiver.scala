package org.spark.streaming.jms

import com.google.common.base.Stopwatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import javax.jms._
import com.spark.streaming.mq._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.BlockGeneratorListener
import org.apache.spark.streaming.receiver.Receiver
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._


trait PublicLogging extends Logging
/*
 * Reliable receiver for a JMS source message provider
*/
abstract class BaseJmsReceiver[T](val consumerFactory: MessageConsumerFactory,
  val messageConverter: (Message) => Option[T],
  override val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
  extends Receiver[T](storageLevel) with Logging {

  @volatile
  @transient
  var receiverThread: Option[ExecutorService] = None

  @volatile
  @transient
  var running = false
  println("Inside Base JMS Receiver")
  //@volatile
  //@transient
  //var blockGenerator: RateLimiter = _

  val stopWaitTime = 1000L

  override def onStop(): Unit = {
    running = false
    consumerFactory.stopConnection()
    receiverThread.foreach(_.shutdown())
    receiverThread.foreach {
      ex =>
        if (!ex.awaitTermination(stopWaitTime, TimeUnit.MILLISECONDS)) {
          ex.shutdownNow()
        }
    }
   /* if (blockGenerator != null) {
      blockGenerator.stop()
      blockGenerator = null
    } */
  }
}


class SynchronousJmsReceiver[T](override val consumerFactory: MessageConsumerFactory,
  override val messageConverter: (Message) => Option[T],
  val batchSize: Int = 1000,
  val maxWait: Duration = 10.second,
  val maxBatchAge: Duration = 30.seconds,
  override val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
  extends BaseJmsReceiver[T](consumerFactory, messageConverter, storageLevel) {
  println("Inside SynchronousJmsReceiver")
  override def onStart(): Unit = {
    running = true
    receiverThread = Some(Executors.newSingleThreadExecutor())
    println(s"Receiver Thread = $receiverThread")
    // We are just using the blockGenerator for access to rate limiter
 //   blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)
    receiverThread.get.execute {
      new Runnable {
        val buffer = ArrayBuffer[Message]()
        val stopWatch = new Stopwatch().start()
        println("Inside Synchronous Receiver runnable interface")
        override def run() = {
          try {
            val consumer = consumerFactory.newConsumer(Session.CLIENT_ACKNOWLEDGE)
            println("New Consumer Created")
            while (running) {
              if (buffer.size >= batchSize || stopWatch.elapsedTime(TimeUnit.MILLISECONDS) >=
             //   stopWatch.elapsed(TimeUnit.MILLISECONDS) >=
                maxBatchAge.toMillis) {
                storeBuffer()
              }
              val message = if (maxWait.toMillis > 0) {
                consumer.receive(maxWait.toMillis)
              } else {
                consumer.receiveNoWait()
              }
              if (message == null && running) {
                storeBuffer()
              } else {
              //  blockGenerator.waitToPush() // Use rate limiter
                buffer += (message)
              }
            }
          } catch {
            case e: Throwable =>
              logError(e.getLocalizedMessage, e)
              restart(e.getLocalizedMessage, e)
          }
        }
        def storeBuffer() = {
          if (buffer.nonEmpty) {
            store(buffer.flatMap(x => messageConverter(x)))
            buffer.last.acknowledge()
            buffer.clear()
          }
          stopWatch.reset()
          stopWatch.start()
        }
      }
    }
  }

/*  class GeneratedBlockHandler extends BlockGeneratorListener {

    override def onAddData(data: Any, metadata: Any): Unit = {}

    override def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {}

    override def onError(message: String, throwable: Throwable): Unit = {}

    override def onGenerateBlock(blockId: StreamBlockId): Unit = {}
  } */

} 

class AsynchronousJmsReceiver[T](override val consumerFactory: MessageConsumerFactory,
  override val messageConverter: (Message) => Option[T],
  val acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE,
  override val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
  extends BaseJmsReceiver[T](consumerFactory, messageConverter, storageLevel) {
  override def onStart(): Unit = {
    running = true
    receiverThread = Some(Executors.newSingleThreadExecutor())
  //  blockGenerator = supervisor.createBlockGenerator(new AsyncGeneratedBlockHandler)

  //  blockGenerator.start()
    receiverThread.get.execute {
      new Runnable {
        val buffer = ArrayBuffer[Message]()

        override def run() = {
          try {
            val consumer = consumerFactory.newConsumer(acknowledgementMode)
            while (running) {
              val message = consumer.receive()
           //   blockGenerator.addData(message)

            }
          } catch {
            case e: Throwable =>
              logError(e.getLocalizedMessage, e)
              restart(e.getLocalizedMessage, e)
          }
        }
      }
    }

  }

  /** Class to handle blocks generated by the block generator. */
/*  private final class AsyncGeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {

    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {

    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      val messages = arrayBuffer.asInstanceOf[mutable.ArrayBuffer[Message]]
      store(messages.flatMap(messageConverter(_)))
      messages.foreach(_.acknowledge())
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    } 
  } */




}

trait MessageConsumerFactory extends Serializable {
  @volatile
  @transient
  var connection: Connection = _
  println("Inside Message Consumer Factory Trait")
  def newConsumer(acknowledgeMode: Int): MessageConsumer = {
    stopConnection()
    println("Making New Connection")
    connection = makeConnection
    println(s"Conenction made as = $connection")
    val session = makeSession(acknowledgeMode)
    println("Making New Consumer")
    val consumer = makeConsumer(session)
    println(s"Consumer made as = $consumer")
    connection.start()
    consumer
  }

  private def makeSession(acknowledgeMode: Int): Session = {
    connection.createSession(false, acknowledgeMode)
  }

  def stopConnection(): Unit = {
    try {
      if (connection != null) {
        connection.close()
      }
    } finally {
      connection = null
    }
  }

  
  def makeConnection: Connection

  def makeConsumer(session: Session): MessageConsumer
}