package akka.actor.mailbox

import com.rabbitmq.client.{ ConnectionFactory, Connection, Channel }
import akka.event.Logging
import akka.actor.{ ActorSystem, ExtendedActorSystem, ActorContext }

/**
 * RabbitMQ maintains a consistent channel from the application to the server
 * but several channels can exist on a single connections.  This class allows
 * us to define a maximum number of connections to multiplex channels through
 */
class RabbitMqChannelPool(settings: RabbitMqBasedMailboxSettings) {
  val maxConnections = settings.maxConnections;
  private var ConnectionArray = new Array[Connection](maxConnections);

  var roundRobinIndex = 0;

  val connectionFactory = new ConnectionFactory();
  //  connectionFactory.setUsername("guest");
  //  connectionFactory.setPassword("guest");
  //  connectionFactory.setVirtualHost("/");
  //  connectionFactory.setHost("localhost");
  //  connectionFactory.setPort(5672);

  connectionFactory.setUsername(settings.username);
  connectionFactory.setPassword(settings.password);
  connectionFactory.setVirtualHost(settings.virtualHost);
  connectionFactory.setHost(settings.host);
  connectionFactory.setPort(settings.port);

  for (i ← 0 until maxConnections) {
    ConnectionArray(i) = connect();
  }

  def createChannel(queueName: String): Channel = {
    //spread the channels across our connections
    roundRobinIndex += 1;
    ConnectionArray(roundRobinIndex % maxConnections).createChannel();
  }

  private def connect() = {
    connectionFactory.newConnection();
  }
}
