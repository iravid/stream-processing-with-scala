val ZIO = "1.0.0-RC21"

lazy val root = (project in file("."))
  .settings(
    name := "stream-processing",
    organization := "com.iravid",
    scalaVersion := "2.13.2",
    scalacOptions := Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-feature",
      "-unchecked",
      "-Ywarn-unused:params,-implicits",
      "-language:higherKinds",
      "-language:existentials",
      "-explaintypes",
      "-Yrangepos",
      "-Xlint:_,-missing-interpolator,-type-parameter-shadow",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard",
      "-Xfatal-warnings"
    ),
    scalacOptions in Compile in console := Seq(),
    initialCommands in Compile in console :=
      """|import zio._
         |import zio.console._
         |import zio.duration._
         |import zio.stream._
         |import zio.Runtime.default._
         |implicit class RunSyntax[A](io: ZIO[ZEnv, Any, A]){ def unsafeRun: A = Runtime.default.unsafeRun(io.provideLayer(ZEnv.live)) }
      """.stripMargin,
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
    libraryDependencies ++= Seq(
      "dev.zio"                %% "zio"          % ZIO,
      "dev.zio"                %% "zio-streams"  % ZIO,
      "dev.zio"                %% "zio-test"     % ZIO % "test",
      "dev.zio"                %% "zio-test-sbt" % ZIO % "test",
      "org.postgresql"         % "postgresql"    % "42.2.14",
      "software.amazon.awssdk" % "s3"            % "2.13.39",
      "org.apache.kafka"       % "kafka-clients" % "2.5.0"
    ),
    scalafmtOnCompile := true
  )
