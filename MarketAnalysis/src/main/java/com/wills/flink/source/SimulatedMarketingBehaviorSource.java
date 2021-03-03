package com.wills.flink.source;

import com.wills.flink.entity.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author 王帅
 * @date 2021-03-01 11:42:20
 * @description:
 *  自定义source
 */
public class SimulatedMarketingBehaviorSource implements SourceFunction<MarketingUserBehavior> {

    // 是否运行的标识位
    Boolean running = true;
    // 定义用户行为和渠道的集合
    List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL",
            "UNINSTALL");
    List<String> channelList = Arrays.asList("app store", "weibo", "wechat",
            "tieba");
    Random random = new Random();

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while(running){
            Long id = random.nextLong();
            String behavior =
                    behaviorList.get(random.nextInt(behaviorList.size()));
            String channel =
                    channelList.get(random.nextInt(channelList.size()));
            Long timestamp = System.currentTimeMillis();
            ctx.collect(new MarketingUserBehavior(id, behavior, channel,timestamp));
            Thread.sleep(random.nextInt(15));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
