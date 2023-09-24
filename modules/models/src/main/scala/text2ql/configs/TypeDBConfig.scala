package text2ql.configs

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class TypeDBConfig(
    url: String,
    keyspaceHr: String,
    keyspaceNTC: String,
    keyspaceHSE: String,
    rules: Boolean = false,
    attributesLimit: Int,
    limit: Int,
    parallel: Boolean,
    transactionTimeoutMillis: Int,
    maxConcurrentTypeDB: Int,
    limitRetries: Int,
    constantDelay: Int,
    testTimes: Int,
    testLimit: Int,
    nPrimaryColumns: Int = 5,
    trustCertCollection: Option[String],
    clientCertChain: Option[String],
    clientPrivateKey: Option[String],
    domain: Option[String],
    useSql: Boolean = true
)

object TypeDBConfig {
  implicit val reader: ConfigReader[TypeDBConfig] = deriveReader
}
