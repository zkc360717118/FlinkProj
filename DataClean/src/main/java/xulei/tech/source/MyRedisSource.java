package xulei.tech.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * redis中存的是 国家和 大区的映射关系
 * hset areas AREA_US US
 * hset areas AREA_CT TW,HK
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN  IN
 *
 *
 * 需要把大区和国家的对应关系组装成java的hashmap
 *
 */
public class MyRedisSource implements SourceFunction<HashMap<String,String>> {
    Logger logger = LoggerFactory.getLogger(MyRedisSource.class);
    private final long SLEEP_MILLION = 60000; //1分钟

    private boolean isRunning = true;
    private Jedis jedis = null;

    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {
        this.jedis =  new Jedis("192.168.0.20",6379,10000);
        //存储所有国家和大区对应的关系
        HashMap<String, String> keyValueMap = new HashMap<String, String>();
        while (isRunning) {
            try {
                //每次先清空hashmap
                keyValueMap.clear();
                //去redis获取结果
                Map<String, String> areas = jedis.hgetAll("areas");

                for(Map.Entry<String,String> entry: areas.entrySet()){
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        keyValueMap.put(split, key);
                    }
                }

                if(keyValueMap.size()>0){
                    ctx.collect(keyValueMap);
                }else{
                    logger.warn("从redis中获取的数据为空！！");
                }
                Thread.sleep(SLEEP_MILLION);
            }catch (JedisConnectionException e) {
                logger.error("redis连接异常，重新获取连接",e.getCause());
                jedis = new Jedis("192.168.0.20",6379,10000);
            }catch (Exception e){
                logger.error("source 的redis数据源异常"+e.getCause());
            }
        }
    }

    public void cancel() {
        isRunning = false;
        if (jedis != null) {
            jedis.close();
        }
    }
}
