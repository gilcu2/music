import sbt.Keys.{tags, _}
import sbt.{Def, _}

object IntegrationTest {
  lazy val IntegrationTestConfig = config("integration") extend Test

  lazy val IntegrationTestSettings: Seq[Def.Setting[_]] = inConfig(IntegrationTestConfig)(Defaults.testSettings) ++ Seq(
    scalaSource in IntegrationTestConfig := baseDirectory.value / "src/test/integration/scala",
    resourceDirectory in IntegrationTestConfig := baseDirectory.value / "src/test/integration/resources",
    fork in IntegrationTestConfig := true,
    parallelExecution in IntegrationTestConfig := true,
    tags in IntegrationTestConfig := Seq((IntegrationTestTag, 1))
  )

  val IntegrationTestTag = Tags.Tag("it")
}
