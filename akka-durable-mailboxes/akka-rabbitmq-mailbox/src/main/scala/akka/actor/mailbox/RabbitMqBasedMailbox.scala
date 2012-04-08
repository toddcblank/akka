package akka.actor.mailbox

import com.typesafe.config.Config
import akka.config.ConfigurationException
import akka.actor.{ ActorRef, ActorContext, ActorSystem }
import akka.dispatch.{ Envelope, MessageQueue, MailboxType }
import akka.event.Logging

import com.rabbitmq.client.{ ConnectionFactory, AMQP }

/**
 * A naive rabbit mq based durable mailbox.  It uses a per-actor queue.  It is possible
 * that the actor could crash after consuming a message.
 *
 * TODO - Integrate better connection handling
 * TODO - Try to remove some of the synchronization
 *
 * @param systemSettings
 * @param config
 */
class RabbitMqBasedMailboxType(systemSettings: ActorSystem.Settings, config: Config) extends MailboxType {
  private val settings = new RabbitMqBaseMailboxSettings(systemSettings, config)
  override def create(owner: Option[ActorContext]): MessageQueue = owner match {
    case Some(o) ⇒ new RabbitMqBasedMessageQueue(o, settings)
    case None    ⇒ throw new ConfigurationException("creating a durable mailbox requires an owner (i.e. does not work with BalancingDispatcher)")
  }
}

class RabbitMqBasedMessageQueue(_owner: ActorContext, val settings: RabbitMqBaseMailboxSettings) extends DurableMessageQueue(_owner) with DurableMessageSerialization {
  val log = Logging(system, "RabbitMqBasedMessageQueue")

  log.info("Prefixing Actor mailboxes with [{}]", settings.queuePrefix);
  var queue = settings.queuePrefix + _owner.self.toString()
  var conn = connect();

  @volatile
  var channel = conn.createChannel();

  channel.exchangeDeclare(settings.exchange, "direct", settings.durable, settings.autoDelete, null)
  channel.queueDeclare(queue, settings.durable, false, settings.autoDelete, null);
  channel.queueBind(queue, settings.exchange, queue);

  var props = new AMQP.BasicProperties()
  if (settings.durable) {
    props.setDeliveryMode(2)
  }

  private var lock = new Object();

  def enqueue(receiver: ActorRef, handle: Envelope) {
    //TODO - See if there's some way to not have to synchronize on all the channel calls
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
        //requeue the message
        channel.basicReject(response.getEnvelope.getDeliveryTag, true)
      }
      response
    }
  }

  def cleanUp(owner: ActorContext, deadLetters: MessageQueue) {
    //Close connection
    //TODO - probably more stuff that I need to clean up here
    channel.close()
    conn.close()
  }

  private def connect() = {
    log.info("CONNECTING to rabbit MQ [{}:{}]", settings.host, settings.port)

    val connectionFactory = new ConnectionFactory();
    connectionFactory.setUsername(settings.username);
    connectionFactory.setPassword(settings.password);
    connectionFactory.setVirtualHost(settings.virtualHost);
    connectionFactory.setHost(settings.host);
    connectionFactory.setPort(settings.port);

    connectionFactory.newConnection();
  }
}