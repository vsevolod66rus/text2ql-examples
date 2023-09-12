package text2ql.configs

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class ServerConfig(
    host: String = "127.0.0.1",
    port: Int = 9001
)

object ServerConfig {
  implicit val reader: ConfigReader[ServerConfig] = deriveReader
}
