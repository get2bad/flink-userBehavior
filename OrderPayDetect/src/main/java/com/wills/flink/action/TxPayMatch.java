package com.wills.flink.action;

import com.wills.flink.entity.OrderEvent;
import com.wills.flink.entity.ReceiptEvent;
import com.wills.flink.process.TxPayMatchDetect;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @author 王帅
 * @date 2021-03-03 16:22:43
 * @description:
 */
public class TxPayMatch {

    private final static OutputTag<OrderEvent> unmatchedPays = new
            OutputTag<OrderEvent>("unmatchedPays") {
            };
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new
            OutputTag<ReceiptEvent>("unmatchedReceipts") {
            };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        URL orderRes = TxPayMatch.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream =
                env.readTextFile(orderRes.getPath())
                        .map(line -> {
                            String[] fields = line.split(",");
                            return new OrderEvent(new Long(fields[0]), fields[1],
                                    fields[2], new Long(fields[3]));
                        })
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                            @Override
                            public long extractAscendingTimestamp(OrderEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }).filter(data -> !"".equals(data.getTxId()));

        URL receiptRes = TxPayMatch.class.getResource("/ReceiptLog.csv");
        DataStream<ReceiptEvent> receiptEventStream = env.readTextFile(receiptRes.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new
                            Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 两条流合并
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>>
                resultStream = orderEventStream
                .keyBy( OrderEvent::getTxId )
                .connect( receiptEventStream.keyBy( ReceiptEvent::getTxId ) )
                .process( new TxPayMatchDetect() );
        resultStream.print("matched");
        resultStream.getSideOutput(unmatchedPays).print("unmatchedPays");

        resultStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts");
        env.execute("tx pay match job");

    }
}
