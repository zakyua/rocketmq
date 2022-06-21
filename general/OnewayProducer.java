package com.atguigu.rocketmq.general;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 *  单向消息发送生产者
 * @author ChenCheng
 * @create 2022-06-21 14:02
 */
public class OnewayProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 1.创建一个Producer，参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 2.指定nameServer的地址
        producer.setNamesrvAddr("192.168.13.128:9876");

        // 3.开启生产者
        producer.start();

        // 4.生产100条信息
        for (int i = 1; i <= 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message message = new Message("OnewayTopic", "OnewayTag", body);
            // 5.单向消息发送
           producer.sendOneway(message);
        }
        // 6.关闭producer
        producer.shutdown();

    }

}
