package xuwei.tech

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.util.Collector
import xuwei.tech.source.MyRedisSourceScala

import scala.collection.mutable

object DataCleanScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
//    env.setParallelism(5)
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置statebackend
//    env.setStateBackend(new RocksDBStateBackend("hdfs://linux01:8020/flink/checkpoints", true))

    //隐式转换
    import org.apache.flink.api.scala._
    val topic = "allData"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","linux01:9092")
    prop.setProperty("group.id","consumer2")

    val myConfumer = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)
    //获取kafka中的数据
    val data = env.addSource(myConfumer)
    //获取最新国家码和大区的映射关系
    val mapData = env.addSource(new MyRedisSourceScala).broadcast//broadcast 可以把数据发送到后面算子的所有并行实例中,注意这里用broadcast（）后面算子就无法使用了
    mapData.print().setParallelism(1)
    val resData = data.connect(mapData).flatMap(new CoFlatMapFunction[String,mutable.Map[String,String],String] {
      //储存国家和大区的隐射关系
      var allMap = mutable.Map[String,String]()
      override def flatMap1(value: String, out: Collector[String]): Unit = {
        val jsonObject = JSON.parseObject(value)
        val dt = jsonObject.getString("dt")
        val countryCode = jsonObject.getString("countryCode")
        //获取大区
        println("大区中map的结果："+allMap.toString())
        println("countryCode："+countryCode)
        val area = allMap.get(countryCode).get
        val jsonArray = jsonObject.getJSONArray("data")
        for(i <- 0 to jsonArray.size()-1) {
          val jsonObject1 = jsonArray.getJSONObject(i)
          jsonObject1.put("area",area)
          jsonObject1.put("dt",dt)
          out.collect(jsonObject1.toString)
          println("进入kafka队列结果是"+jsonObject1.toString)

        }
      }

      override def flatMap2(value: mutable.Map[String, String], out: Collector[String]): Unit = {
        this.allMap = value
      }
    })

    val outTopic = "allDataClean"
    val outprop = new Properties
    outprop.put("bootstrap.servers", "linux01:9092")
    //设置事务超时时间
    outprop.setProperty("transaction.timeout.ms", 60000 * 15 + "")

    val myProducer = new FlinkKafkaProducer011[String](outTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema), outprop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
    resData.addSink(myProducer)

    env.execute("DataCleanScala")
  }
}
