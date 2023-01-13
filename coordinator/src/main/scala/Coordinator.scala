import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
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

//  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9085)
//
//  EmbeddedKafka.start()

  val props = new Properties
  props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9085")

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

  Runtime.getRuntime.addShutdownHook(
    {
//      EmbeddedKafka.stop()
      new Thread("streams-wordcount-shutdown-hook") {
        override def run(): Unit = {
          processes.asScala.iterator.foreach { p =>
            p._2.destroy()
          }
        }
      }
    }  )

  Thread.sleep(2000)

  //    runProgram(Seq(6, 7, 7, 0, 4, 6)

  val t = new Thread(() =>
//    runProgram(Seq(6, 7, 7, 0, 4, 6))
    runProgram(Seq(7, 6, 2))
//    runProgram(LazyList.from(Iterator.continually(scala.util.Random.nextInt(10))))
  )
  t.setDaemon(true)
  t.start()

  (1 to 1000000).foreach { i =>
    1 to 100 foreach { x =>
      producer.send(
        new ProducerRecord[String, Integer](s"$appId-input-topic", s"$x", i)
      )
      Thread.sleep(5)
    }

//    EmbeddedKafka.stop()
  }

  def runProgram(p: Seq[Int]): Unit = {
    p.zipWithIndex.foreach { case (i, pos) =>
      if (processes.containsKey(i)) {
        println(s"[$pos] -$i")
        val pr = processes.remove(i)
        pr.destroy()
      } else {
        println(s"[$pos] +$i")

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
