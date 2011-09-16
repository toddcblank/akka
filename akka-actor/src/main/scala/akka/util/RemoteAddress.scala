/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.util

import java.net.InetSocketAddress

object RemoteAddress {
  def apply(inetAddress: InetSocketAddress): RemoteAddress = inetAddress match {
    case null ⇒ null
    case inet ⇒ new RemoteAddress(inet.getAddress.getHostAddress, inet.getPort)
  }
}

case class RemoteAddress(val hostname: String, val port: Int)
