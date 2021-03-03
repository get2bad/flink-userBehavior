package com.wills.flink.action.sql;

import com.wills.flink.action.HotPages;
import com.wills.flink.entity.ApacheLogEvent;
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

import java.net.URL;
import java.text.SimpleDateFormat;

/**
 * @author 王帅
 * @date 2021-02-23 14:25:49
 * @description:
 *
 *  使用 Flink Sql的方式来实现 热门页的展示
 */
public class HotPagesWithSQL {

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

        // 将转化的 DataStream 弄成临时表
        tableEnv.createTemporaryView("data_table", dataStream,"method as m, timestamp.rowtime as ts , url");

        Table hotPages = tableEnv.sqlQuery("Select url,cnt from (" +
                "select " +
                "url,cnt,ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num " +
                "from (" +
                "Select url,count(url) as cnt, HOP_END(ts,interval '5' second, interval '1' minute) as windowEnd " +
                "from data_table where m = 'GET' " +
                "and url not like '%.png' " +
                "and url not like '%.css' " +
                "and url not like '%.js' " +
                "and url not like '%.txt' " +
                "and url not like '%.ico' " +
                "and url not like '%.jpg' " +
                "and url not like '%.ttf' " +
                "and url not like '%.rb' " +
                "and url not like '%.html' " +
                "and url not like '%.jar' " +
                "group by url,HOP(ts,interval '5' second, interval '1' minute)  " +
                ")" +
                ") where row_num <= 5");
        tableEnv.toRetractStream(hotPages, Row.class).print("");

        env.execute("NetworkFlow");
    }
}