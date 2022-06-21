package com.atguigu.rocketmq.general;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消息消费者
 * @author ChenCheng
 * @create 2022-06-21 14:08
 */
public class SomeConsumer {

    public static void main(String[] args) throws MQClientException {
        // 1. 定义一个push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        // 2.指定nameServer
        consumer.setNamesrvAddr("192.168.13.128:9876");
        // 3.指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 4. 指定消费topic与tag
        //consumer.subscribe("SyncTopic","*");
         consumer.subscribe("AsyncTopic","*");
        // consumer.subscribe("OnewayTopic","*");

        // 5.注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 一旦broker中有了其订阅的消息就会触发该方法的执行，其返回值为当前consumer消费的状态
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                // 6.消费信息
                for (MessageExt msg : msgs) {
                    System.out.println(msg);
                }
                // 7.返回消费状态：消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 8.开启消费者消费
        consumer.start();

    }

}
