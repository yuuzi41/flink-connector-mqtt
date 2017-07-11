package org.apache.flink.streaming.connectors.mqtt


import java.util.Locale

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3._
import org.slf4j.LoggerFactory

@SerialVersionUID(1L)
class MQTTSink[IN](config: MQTTSinkConfig[IN]) extends RichSinkFunction[IN] with MqttCallback {
  val log = LoggerFactory.getLogger(classOf[MQTTSource[IN]])

  val uri = config.uri
  val clientIdPrefix = config.clientIdPrefix
  val topic = config.topic
  val qos = config.qos
  val retained = config.retained
  val serializationSchema = config.serializationSchema

  var client: MqttClient = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val connOpts = new MqttConnectOptions
    connOpts.setCleanSession(true)

    // usual brokers(such as mosquitto) are disconnecting clients that have same client id.
    //   because brokers judges it to be an old connection.
    // so this implementation generate unique client ids for each threads.
    val genClientId = "%s_%04d%05d".format(
      clientIdPrefix,
      Thread.currentThread().getId % 10000,
      System.currentTimeMillis % 100000
    )

    client = new MqttClient(uri, genClientId, new MemoryPersistence())
    client.connect(connOpts)

    client.setCallback(this)

    log.info("connect")
  }

  override def invoke(value: IN): Unit = {
    try {
      val bytes = serializationSchema.serialize(value)
      client.publish(topic, bytes, qos, retained)
    } catch {
      case e: MqttException => throw new RuntimeException("Failed to publish", e)
    }
  }

  override def close(): Unit = {
    super.close()
    client.close()
  }

  override def connectionLost(cause: Throwable): Unit = {
    throw cause
  }

  override def deliveryComplete(token: IMqttDeliveryToken): Unit = {}

  override def messageArrived(topic: String, message: MqttMessage): Unit = {}
}
