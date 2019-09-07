import sbt.Keys._
import sbt.{Def, _}

object UnitTest {
  lazy val UnitTestSettings: Seq[Def.Setting[_]] = inConfig(Test)(Defaults.testSettings) ++ Seq(
    scalaSource in Test := baseDirectory.value / "src/test/unit/scala",
    resourceDirectory in Test := baseDirectory.value / "src/test/unit/resources",
    fork in Test := true,
    parallelExecution in Test := true
  )
}
