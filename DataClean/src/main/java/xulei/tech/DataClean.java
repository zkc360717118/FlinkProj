package xulei.tech;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;
import xulei.tech.source.MyRedisSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * 数据清洗需求
 */
public class DataClean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint配置
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend

//        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));


        //指定kafkaSource
        String topic = "allData";
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "linux01:9092");
        prop.put("group.id", "con1");
        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), prop);

        //获取kafka中的数据'
        //{"dt":"2018-01-01 11:11:11","countryCode:"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s2","score":0.1,"level":"B"}]}
        DataStreamSource<String> data = env.addSource(myConsumer);

        // 最新的国家码 和大区的映射关系
        DataStreamSource<HashMap<String, String>> mapData = env.addSource(new MyRedisSource());

        //把data数据和 mapData数据连接整合
        SingleOutputStreamOperator<String> resData = data.connect(mapData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
            //存储国家和大区的映射关系  为了可以在flatMap1方法中调用
            private HashMap<String, String> allMap = new HashMap<String, String>();

            //处理kafka中的数据
            public void flatMap1(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");
                //获取大区
                String area = allMap.get(countryCode);

                //把大区放入每一个子集json中
                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                    jsonObject1.put("area", area);
                    jsonObject1.put("dt", dt);
                    out.collect(jsonObject.toJSONString()); //输出
                }
            }

            //处理的是redis返回的map类型的数据
            public void flatMap2(HashMap<String, String> value, Collector<String> out) throws Exception {
                this.allMap = value;
            }
        });

        //处理sink
        String outTopic = "allDataClean";
        Properties outprop = new Properties();
        outprop.put("bootstrap.servers", "linux01:9092");
        //设置事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(outTopic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), outprop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
        resData.addSink(myProducer);

        env.execute("dataclean");

    }
}
