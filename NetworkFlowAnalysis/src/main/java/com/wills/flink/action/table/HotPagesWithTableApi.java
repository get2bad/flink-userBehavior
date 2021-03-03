package com.wills.flink.action.table;

import com.wills.flink.action.HotPages;
import com.wills.flink.aggregate.PageCountAgg;
import com.wills.flink.entity.ApacheLogEvent;
import com.wills.flink.entity.PageViewCount;
import com.wills.flink.process.TopNHotPages;
import com.wills.flink.window.PageCountResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * @author 王帅
 * @date 2021-02-23 14:25:49
 * @description:
 *
 *  使用 Table API的方式来实现 热门页的展示
 */
public class HotPagesWithTableApi {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        URL resource = HotPages.class.getResource("/apache.log");
        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        // 转换为实体类
        SingleOutputStreamOperator<ApacheLogEvent> dataStream = source.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5],
                    fields[6]);
            // 1分钟的 water mark 的延迟时间
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getTimestamp();
            }
        });

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 操作dataStream
        Table table = tableEnv.fromDataStream(dataStream, "method as m, timestamp.rowtime as ts , url");

        Table res = table.filter(" m === 'GET'")
                .window(
                        Slide.over("10.minutes")
                                .every("5.seconds")
                                .on("ts")
                                .as("w"))
                .groupBy("m,w,url")
                .select("m, w.end as windowEnd,url,url.count as cnt");

        tableEnv.createTemporaryView("hotPages",tableEnv.toAppendStream(res,Row.class),"m,windowEnd,url,cnt");

        Table hotPages = tableEnv.sqlQuery("Select m,cnt from (" +
                "select " +
                "m,cnt,ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num " +
                "from hotPages" +
                ") where row_num <= 5");
        tableEnv.toRetractStream(hotPages, Row.class).print("");

        env.execute("NetworkFlow");
    }
}
