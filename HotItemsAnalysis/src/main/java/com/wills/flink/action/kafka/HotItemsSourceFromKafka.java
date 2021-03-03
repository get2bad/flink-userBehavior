package com.wills.flink.action.kafka;

import com.wills.flink.aggregate.CountAgg;
import com.wills.flink.entity.ItemViewCount;
import com.wills.flink.entity.UserBehavior;
import com.wills.flink.process.TopNHotItems;
import com.wills.flink.window.WindowResultFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author 王帅
 * @date 2021-02-22 14:23:54
 * @description:
 */
public class HotItemsSourceFromKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 告诉flink 要使用业务时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "tx:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> source =  env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));

        // 转换为实体类
        SingleOutputStreamOperator<UserBehavior> dataStream = source.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new
                    Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() { // 注册 water mark 水位线
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 过滤出点击事件
        SingleOutputStreamOperator<UserBehavior> clickDataStream = dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        });

        // 进行归类聚合
        SingleOutputStreamOperator<ItemViewCount> keyByDStream = clickDataStream.keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5)) // 滑动窗口，以1小时为临界点 每次滑动5分钟
                .aggregate(new CountAgg(), new WindowResultFunction()); // 聚合操作

        // 聚合完了 进行处理
        SingleOutputStreamOperator<String> res = keyByDStream.keyBy("windowEnd")
                                                    .process(new TopNHotItems(3));

        // 添加输出的 producer 到 kafka 中
        res.addSink(new FlinkKafkaProducer011("tx:9092","hotitems",new SimpleStringSchema()));

        env.execute("hotItems");
    }
}
