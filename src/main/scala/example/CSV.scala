package example

import cats.effect._
import fs2._
import fs2.io.file._
import fs2.data.csv._
import fs2.data.text.utf8._

object CSV {
  def stream(filePath: String): Stream[IO, (String, Long)] = Files[IO]
    .readAll(Path(filePath))
    .through(decodeWithoutHeaders[MedScan]('\t'))
    .map(medScan => medScan.id.map(medScan.hitId -> _))
    .unNone
}
