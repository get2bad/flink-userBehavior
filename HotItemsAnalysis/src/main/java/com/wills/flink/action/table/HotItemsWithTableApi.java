package com.wills.flink.action.table;

import com.wills.flink.action.HotItems;
import com.wills.flink.entity.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;

/**
 * @author 王帅
 * @date 2021-02-22 18:08:52
 * @description:
 * 使用sql的方式完成 热门商品的查询
 */
public class HotItemsWithTableApi {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

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
                return element.getTimestamp() * 1000L;
            }
        });

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId,behavior,timestamp.rowtime as ts");


        // 分组开窗
        Table windowAggTable = dataTable.filter("behavior === 'pv'")
                .window(Slide.over("1.hours")
                        .every("5.minutes")
                        .on("ts")
                        .as("w"))
                .groupBy("itemId,w")
                .select("itemId,w.end as windowEnd,itemId.count as cnt");

        DataStream<Row> aggDStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        aggDStream.print();

        env.execute();
    }
}
