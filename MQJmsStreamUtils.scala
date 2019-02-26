package com.spark.streaming.mq

import javax.jms._
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.spark.streaming.jms._
import com.ibm.mq.jms.MQQueue
import com.ibm.mq.jms.MQQueueConnection
import com.ibm.mq.jms.MQQueueConnectionFactory
import com.ibm.msg.client.wmq.WMQConstants
import com.ibm.msg.client.wmq.compat.jms.internal.JMSC
import com.ibm.msg.client.jms.JmsFactoryFactory

case class MQConsumerFactory(mqHost: String,
                            mqPort: Int,
                            mqQmgr: String,
                            mqQname: String,
                            mqUser: String,
                            mqCred: String,
                            connectionFactoryName: String = "ConnectionFactory",
                            messageSelector: String = "")
extends MessageConsumerFactory with Logging {
    
    @volatile
    @transient
    var host: String = _
    var port: Int = _
    var qmgr: String = _
    var qname: String = _
    var uname: String = _
    var creds: String = _
    
    println(s"MQ Hostname = $mqHost")
    println(s"MQ Portname = $mqPort")
    println("Inside MQ Consumer Factory Method")
    
    override def makeConsumer(session: Session): MessageConsumer = {
        var queue = new MQQueue
        queue = session.createQueue(qname).asInstanceOf[MQQueue]
        session.createConsumer(queue, messageSelector)
    }
    
    println("Consumer Created")
     
    override def makeConnection: Connection = {
        if (host == null){
            host = mqHost
            port = mqPort
            qmgr = mqQmgr
            qname = mqQname
            uname = mqUser
            creds = mqCred
        }
        val conFactory = new MQQueueConnectionFactory()
        println(s"MQ Hostname = $host")
        conFactory.setHostName(host)
        println(s"MQ Portname = $port")
        conFactory.setPort(port)
        //conFactory.setChannel("INFORMATICA.SVRCONN")
        conFactory.setChannel("MQEXPLORER.SVRCONN")
        conFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP)
        conFactory.setQueueManager(qmgr)
        
        val qCon = conFactory.createQueueConnection(uname, creds).asInstanceOf[MQQueueConnection]
        
        qCon
    }
    println("Connection Created")
}
