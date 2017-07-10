package org.apache.flink.streaming.connectors.mqtt


import org.apache.flink.streaming.util.serialization.DeserializationSchema
import org.apache.flink.util.Preconditions

class MQTTSourceConfig[OUT](
                             uriArg: String,
                             clientIdArg: String,
                             topicArg: String,
                             qosArg: Int,
                             deserializationSchemaArg: DeserializationSchema[OUT]
                           ) {

  val uri = Preconditions.checkNotNull(uriArg, "uri not set")
  val clientId = Preconditions.checkNotNull(clientIdArg, "clientId not set")
  val topic = Preconditions.checkNotNull(topicArg, "topic not set")
  val qos = Preconditions.checkNotNull(qosArg, "qos not set")
  val deserializationSchema = Preconditions.checkNotNull(deserializationSchemaArg, "deserializationSchema not set")
}
object MQTTSourceConfig {
  class Builder[OUT] {
    var uri: String = _
    var clientId: String = _
    var topic: String = _
    var qos: Int = _
    var deserializationSchema: DeserializationSchema[OUT] = _

    def setURI(uri: String): Builder[OUT] = {
      this.uri = uri
      this
    }

    def setClientId(clientId: String): Builder[OUT] = {
      this.clientId = clientId
      this
    }

    def setTopic(topic: String): Builder[OUT] = {
      this.topic = topic
      this
    }


    def setQos(qos: Int): Builder[OUT] = {
      this.qos = qos
      this
    }

    def setDeserializationSchema(deserializationSchema: DeserializationSchema[OUT]): Builder[OUT] = {
      this.deserializationSchema = deserializationSchema
      this
    }

    def build(): MQTTSourceConfig[OUT] = {
      new MQTTSourceConfig[OUT](uri, clientId, topic, qos, deserializationSchema)
    }
  }

}
