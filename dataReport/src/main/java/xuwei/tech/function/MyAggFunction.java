package xuwei.tech.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

public class MyAggFunction implements WindowFunction<Tuple3<Long, String, String>, Tuple4<String, String, String, Long>, Tuple, TimeWindow>{
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<Long, String, String>> input, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
        //1获取上面key分组字段信息
        String type = tuple.getField(0).toString();
        String area = tuple.getField(1).toString();

        //2循环处理当前窗口中的数据
        Iterator<Tuple3<Long, String, String>> it = input.iterator();
        ArrayList<Long> arrayList = new ArrayList<>(); //把时间用来排序，获取当前窗口最大时间，存入es

        long count = 0;
        while (it.hasNext()) {
            Tuple3<Long, String, String> next = it.next();
            arrayList.add(next.f0);
            count++;
        }
        System.err.println(Thread.currentThread().getId()+",window触发了，数据条数："+count);

        //3 排序+获取最大时间+转化时间格式方便es中查看
        Collections.sort(arrayList);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = sdf.format(new Date(arrayList.get(arrayList.size() - 1)));

        //4 组装结果
        Tuple4<String, String, String, Long> res = new Tuple4<>(time, type, area, count);
        out.collect(res);
    }
}

