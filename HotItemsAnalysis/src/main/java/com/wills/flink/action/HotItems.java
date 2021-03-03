package com.wills.flink.action;

import com.wills.flink.aggregate.CountAgg;
import com.wills.flink.entity.ItemViewCount;
import com.wills.flink.entity.UserBehavior;
import com.wills.flink.process.TopNHotItems;
import com.wills.flink.window.WindowResultFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * @author 王帅
 * @date 2021-02-22 14:23:54
 * @description:
 */
public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 告诉flink 要使用业务时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = HotItems.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        // 转换为实体类
        SingleOutputStreamOperator<UserBehavior> dataStream = source.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new
                    Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() { // 注册 water mark 水位线
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L; // 确定是以业务时间而 水位线的基础
            }
        });

        // 过滤出点击事件
        SingleOutputStreamOperator<UserBehavior> clickDataStream = dataStream.filter(data -> data.getBehavior().equals("pv"));

        // 进行归类聚合
        /**
         * CountAgg : 聚合自己实现类 实现了 AggregateFunction<UserBehavior,Long,Long> 接口
         * WindowResultFunction： 窗口结果处理方法 实现了 WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> 方法
         */
        SingleOutputStreamOperator<ItemViewCount> keyByDStream = clickDataStream.keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5)) // 滑动窗口，以1小时为临界点 每次滑动5分钟
                .aggregate(new CountAgg(), new WindowResultFunction()); // 聚合操作

        // 聚合完了 进行处理
        SingleOutputStreamOperator<String> res = keyByDStream.keyBy("windowEnd")
                                                    .process(new TopNHotItems(3));

        res.print();

        env.execute("hotItems");
    }
}
