package streams

package object workshop {
  type ??? = Nothing

  implicit val ops = scala.language.postfixOps

  implicit class Pending[A](a: A) {
    def ? : Nothing = ???
  }
}
