package com.qijiabin.RocketMQDemo;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;

/**
 * ========================================================
 * 日 期：2016年12月6日 上午9:53:42
 * 版 本：1.0.0
 * 类说明：消息生产者
 * TODO
 * ========================================================
 * 修订日期     修订人    描述
 */
public class Producer {

	private static final DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
	private static int initialState = 0;

	public static DefaultMQProducer getDefaultMQProducer() {
		if (initialState == 0) {
			producer.setNamesrvAddr("192.168.1.17:9876");
			try {
				producer.start();
			} catch (MQClientException e) {
				e.printStackTrace();
				return null;
			}

			initialState = 1;
		}

		return producer;
	}

}

