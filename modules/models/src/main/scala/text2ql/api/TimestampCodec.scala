package text2ql.api

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}

import java.sql.Timestamp

trait TimestampCodec {

  implicit protected val timestampFormatFromLong: Encoder[Timestamp] with Decoder[Timestamp] = new Encoder[Timestamp]
    with Decoder[Timestamp] {
    override def apply(a: Timestamp): Json = Encoder.encodeLong.apply(a.getTime)

    override def apply(c: HCursor): Result[Timestamp] = Decoder.decodeLong.map(s => new Timestamp(s)).apply(c)
  }
}
