package xuwei.tech.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable

class MyRedisSourceScala extends SourceFunction[mutable.Map[String,String]]{
  val logger = LoggerFactory.getLogger("MyRedisSourceScala")
  val SLEEP_MILLION = 60000
  var isRunning = true
  var jedis:Jedis = _

  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, String]]): Unit = {
    this.jedis = new Jedis("192.168.0.20",6379,10000)
    //隐式转换把java的hashmap转化为scala的map
    import scala.collection.JavaConversions.mapAsScalaMap

    //存储所有国家和大区的对应关系
    var keyValueMap = mutable.Map[String,String]()
    while(isRunning){
      try{
        keyValueMap.clear()
        var areas:mutable.Map[String,String] = jedis.hgetAll("areas")  //如果没有上面的隐式转换这里要报错

        for(key <- areas.keys.toList){
          val value = areas.get(key).get
          val splits = value.split(",")
          for(split <- splits){
            keyValueMap += (split->key)
          }
        }

        if(keyValueMap.nonEmpty){
          print("最后的结果"+ keyValueMap)
          ctx.collect(keyValueMap)
        }else{
          logger.warn("从redis获取的数据为空")
        }
        Thread.sleep(SLEEP_MILLION)
      }catch {
        case e: JedisConnectionException=>{
          logger.error("redis连接异常，开始重新连接"+e.getCause)
          jedis = new Jedis("192.168.0.20",6379,10000)
        }
        case e: Exception =>{
          logger.error("source 数据源异常"+e.getMessage)
        }
      }
    }
  }

  override def cancel(): Unit = {
    isRunning = false
    if(jedis !=null){
      jedis.close()
    }
  }
}
