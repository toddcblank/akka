package akka.actor.mailbox

import com.typesafe.config.Config
import akka.config.ConfigurationException
import akka.actor.{ ActorRef, ActorContext, ActorSystem }
import akka.dispatch.{ Envelope, MessageQueue, MailboxType }
import akka.event.Logging
import com.rabbitmq.client.{ Channel, AMQP, ConnectionFactory }

/**
 * A naive rabbit mq based durable mailbox.  It uses a per-actor queue.  It is possible
 * that the actor could crash after consuming a message.
 *
 * TODO - Integrate better connection handling
 *
 * @param systemSettings
 * @param config
 */
class RabbitMqBasedMailboxType(systemSettings: ActorSystem.Settings, config: Config) extends MailboxType {
  private val settings = new RabbitMqBasedMailboxSettings(systemSettings, config)

  override def create(owner: Option[ActorContext]): MessageQueue = owner match {
    case Some(o) ⇒ new RabbitMqBasedMessageQueue(o, settings)
    case None    ⇒ throw new ConfigurationException("creating a durable mailbox requires an owner (i.e. does not work with BalancingDispatcher)")
  }
}

class RabbitMqBasedMessageQueue(_owner: ActorContext, val settings: RabbitMqBasedMailboxSettings) extends DurableMessageQueue(_owner) with DurableMessageSerialization {

  val log = Logging(system, "RabbitMqBasedMessageQueue")

  log.info("Prefixing Actor mailboxes with [{}]", settings.queuePrefix);
  val queue = settings.queuePrefix + _owner.self.toString()

  log.info("Using queue [{}]", queue);

  log.info("Creating Channel");
  @volatile
  var channel = settings.channelPool.createChannel(queue)

  channel.exchangeDeclare(settings.exchange, "direct", settings.durable, settings.autoDelete, null)
  channel.queueDeclare(queue, settings.durable, false, settings.autoDelete, null);
  channel.queueBind(queue, settings.exchange, queue);

  val props = new AMQP.BasicProperties()
  if (settings.durable) {
    props.setDeliveryMode(2)
  }

  private val lock = new Object();

  def enqueue(receiver: ActorRef, handle: Envelope) {
    lock.synchronized {
      channel.basicPublish(settings.exchange, queue, props, serialize(handle))
    }
  }

  def dequeue(): Envelope = try {
    lock.synchronized {
      val response = channel.basicGet(queue, true)

      if (response == null) {
        null
      } else {
        deserialize(response.getBody)
      }
    }
  }

  def numberOfMessages = {
    val response = peek
    if (response != null) {
      //this is technically just an estimate from RabbitMQ, but it's the best guess we have
      response.getMessageCount
    } else {
      0
    }
  }

  def hasMessages = {
    peek != null
  }

  private def peek = {
    lock.synchronized {
      val response = channel.basicGet(queue, false)
      if (response != null) {
        //requeue the message, this does have the side effect of messages
        //being processed out of order, but that's something that consumers
        //should be handling anyway.
        channel.basicReject(response.getEnvelope.getDeliveryTag, true)
      }
      response
    }
  }

  def cleanUp(owner: ActorContext, deadLetters: MessageQueue) {
    //Close channel
    channel.close()
  }

}