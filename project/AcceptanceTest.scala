import sbt.Keys._
import sbt.{Def, _}

object AcceptanceTest {
  lazy val AcceptanceTestConfig: Configuration = config("acceptance") extend IntegrationTest.IntegrationTestConfig

  lazy val AcceptanceTestSettings: Seq[Def.Setting[_]] = inConfig(AcceptanceTestConfig)(Defaults.testSettings) ++ Seq(
    scalaSource in AcceptanceTestConfig := baseDirectory.value / "src/test/acceptance/scala",
    resourceDirectory in AcceptanceTestConfig := baseDirectory.value / "src/test/acceptance/resources",
    fork in AcceptanceTestConfig := true,
    parallelExecution in AcceptanceTestConfig := false,
    javaOptions in AcceptanceTestConfig ++= Seq(
      "-Dcom.sun.management.jmxremote",
      "-Dcom.sun.management.jmxremote.port=9191",
      "-Dcom.sun.management.jmxremote.rmi.port=9191",
      "-Dcom.sun.management.jmxremote.authenticate=false",
      "-Dcom.sun.management.jmxremote.ssl=false",
      "-Djava.rmi.server.hostname=localhost"
    )
  )
}
