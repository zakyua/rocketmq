package com.atguigu.rocketmq.batch;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 批量消息消费者
 * @author ChenCheng
 * @create 2022-06-21 16:24
 */
public class BatchConsumer {
    public static void main(String[] args) throws MQClientException {
        // 1. 定义一个push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        // 2.指定nameServer
        consumer.setNamesrvAddr("192.168.13.128:9876");
        // 3.指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 4. 指定消费topic与tag
        consumer.subscribe("BatchTopic","*");
        // 5.指定每次可以消费10条消息，默认为1
        consumer.setConsumeMessageBatchMaxSize(10);
        // 6.指定每次可以从Broker拉取40条消息，默认为32
        consumer.setPullBatchSize(40);
        // 7.注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                // 8.消费信息
                for (MessageExt msg : msgs) {
                    System.out.println(msg);
                }
                // 9.返回消费状态：消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 10.开启消费者消费
        consumer.start();

    }
}
