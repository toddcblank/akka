package akka.actor.mailbox

import akka.actor.ActorSystem
import com.typesafe.config.Config

class RabbitMqBaseMailboxSettings(val systemSettings: ActorSystem.Settings, val userConfig: Config)
  extends DurableMailboxSettings {

  def name = "rabbitmq-based"
  val config = initialize
  import config._

  val host = getString("host")
  val port = getInt("port")
  val virtualHost = getString("virtualHost")
  val username = getString("username")
  val password = getString("password")
  val exchange = getString("exchange")
  val durable = getBoolean("durable")
  val autoDelete = getBoolean("autoDelete")
  val queuePrefix = getString("queuePrefix")

}
