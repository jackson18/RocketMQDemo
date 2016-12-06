package com.qijiabin.RocketMQDemo;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * ========================================================
 * 日 期：2016年12月6日 上午9:56:44
 * 版 本：1.0.0
 * 类说明：消息消费者
 * TODO
 * ========================================================
 * 修订日期     修订人    描述
 */
public class Consumer {
	
	private static final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroupName");
    private static int initialState = 0;

    public static DefaultMQPushConsumer getDefaultMQPushConsumer(){     
        if(initialState == 0){
            consumer.setNamesrvAddr("172.17.42.1:9876");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            initialState = 1;
        }

        return consumer;        
    }

}
