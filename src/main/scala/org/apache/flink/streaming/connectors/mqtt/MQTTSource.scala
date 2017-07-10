package org.apache.flink.streaming.connectors.mqtt

import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{MessageAcknowledgingSourceBase, SourceFunction}
import org.eclipse.paho.client.mqttv3._
import org.slf4j.LoggerFactory

import scala.collection.convert.WrapAsScala._

class MQTTSource[OUT](config: MQTTSourceConfig[OUT])
  extends MessageAcknowledgingSourceBase[OUT, String](classOf[String])
    with ResultTypeQueryable[OUT]
    with MqttCallback {

  val log = LoggerFactory.getLogger(classOf[MQTTSource[OUT]])

  val uri = config.uri
  val clientId = config.clientId
  val topic = config.topic
  val qos = config.qos
  val deserializationSchema = config.deserializationSchema

  var client: MqttClient = _
  var running: AtomicBoolean = new AtomicBoolean(false)
  val queue = new LinkedBlockingQueue[(String, MqttMessage)]
  val unacknowledgedMessages = new util.HashMap[String, MqttMessage]()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    client = new MqttClient(uri, clientId)
    client.connect()

    // todo: support multiple topic
    client.subscribe(topic, qos)
    client.setCallback(this)
    running.set(true)

    log.info("connect")
  }

  override def close(): Unit = {
    super.close()

    client.close()
    running.set(false)

    log.info("disconnect")
  }

  override def acknowledgeIDs(checkpointId: Long, uIds: util.List[String]): Unit = {
    uIds.foreach { id =>
      unacknowledgedMessages.synchronized {
        val message = unacknowledgedMessages.get(id)
        message.synchronized {
          //message.notify()
          unacknowledgedMessages.remove(id)
        }
      }
    }
  }

  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {
    while (running.get()) {
      val message = queue.take()
      val msg = message._2
      val value = deserializationSchema.deserialize(msg.getPayload)

      ctx.getCheckpointLock.synchronized {
        ctx.collect(value)
        if (msg.getQos > 0) {
          val id = message.hashCode().toString
          addId(id)
          unacknowledgedMessages.synchronized {
            unacknowledgedMessages.put(id, msg)
          }
        }
      }
    }
  }

  override def cancel(): Unit = {
    this.running.set(false)
  }

  override def getProducedType: TypeInformation[OUT] = {
    deserializationSchema.getProducedType
  }

  override def connectionLost(cause: Throwable): Unit = {
    throw cause
  }

  override def deliveryComplete(token: IMqttDeliveryToken): Unit = {}

  override def messageArrived(topic: String, message: MqttMessage): Unit = {
    queue.put((topic,message))
  }
}
