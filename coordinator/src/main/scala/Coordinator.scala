import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import os.{Path, SubProcess}

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

object Coordinator extends scala.App {

  val workerJar = "worker/target/scala-2.13/worker.jar"

  val appId = java.util.UUID.randomUUID().toString
  println(s"appid: $appId")

  val props = new Properties
  props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val adminClient = AdminClient.create(props)
  val producer = new KafkaProducer(
    props,
    Serdes.String().serializer(),
    Serdes.Integer().serializer()
  )

  adminClient.createTopics(
    Seq(
      new NewTopic(s"$appId-output-topic", 30, 1.shortValue()),
      new NewTopic(s"$appId-input-topic", 30, 1.shortValue())
    ).asJava
  )

  val processes = new ConcurrentHashMap[Int, SubProcess]()

//   Runtime.getRuntime.addShutdownHook({
// //      EmbeddedKafka.stop()
//     new Thread("streams-wordcount-shutdown-hook") {
//       override def run(): Unit = {
//         processes.asScala.iterator.foreach { p =>
//           p._2.destroy()
//         }
//       }
//     }
//   })

//   Thread.sleep(5000)

  val t = new Thread(() =>
    if (args.length > 0) {
      val program = args.map(_.toInt)
      runProgram(program)
    } else {
      runProgram(
        LazyList.from(Iterator.continually(scala.util.Random.nextInt(10)))
      )
    }
  )

  t.setDaemon(true)
  t.start()

  (1 to 5000000).foreach { i =>
    (1 to 5000000).foreach { x =>
      producer.send(
          new ProducerRecord[String, Integer](s"$appId-input-topic", s"$x", i)
      )
    }
    Thread.sleep(1)
  }


  println(s"done instance")

  def runProgram(p: Seq[Int]): Unit = {
    Thread.sleep(5000)
    p.zipWithIndex.foreach { case (i, pos) =>
      if (processes.containsKey(i)) {
        println(s"[$pos] stopping instance $i")
        val pr = processes.remove(i)
        pr.destroy()
      } else {
        println(s"[$pos] starting instance +$i")

        processes.put(
          i,
          os.proc(Seq("java", "-jar", workerJar))
            .spawn(
              env = Map(("APPLICATION_ID", appId), ("INSTANCE_ID", s"$pos")),
              stdout = os.PathRedirect(Path(s"$appId.$pos.log", os.pwd))
            )
        )
      }
      Thread.sleep(15000)
    }

  }

}
