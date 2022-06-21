package com.atguigu.rocketmq.filter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * SQL过滤Consumer
 *  默认情况下Broker没有开启消息的SQL过滤功能，需要在Broker加载的配置文件中添加如下属性，以开启该功能：
 *  enablePropertyFilter = true
 *  开启broker
 *  nohup sh bin/mqbroker -n localhost:9876 -c conf/broker.conf &
 *
 * @author ChenCheng
 * @create 2022-06-21 17:05
 */
public class FilterBySQLConsumer {
    public static void main(String[] args) throws MQClientException {
        // 1. 定义一个push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        // 2.指定nameServer
        consumer.setNamesrvAddr("192.168.13.128:9876");
        // 3.指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 4.指定消费topic与tag
        consumer.subscribe("FilterBySQLTopic", MessageSelector.bySql("age between 0 and 6"));
        // 5.注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // 6.消费信息
                for (MessageExt me:msgs){
                    System.out.println(me);
                }
                // 7.返回消费状态：消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 8.开启消费者消费
        consumer.start();


    }
}
