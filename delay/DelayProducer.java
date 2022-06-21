package com.atguigu.rocketmq.delay;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 延时消息
 * @author ChenCheng
 * @create 2022-06-21 14:52
 */
public class DelayProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1.创建一个Producer，参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 2.指定nameServer的地址
        producer.setNamesrvAddr("192.168.13.128:9876");
        // 3.开启生产者
        producer.start();
        // 4.生产100条信息
        for (int i = 1; i <= 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message message = new Message("DelayTopic", "DelayTag", body);
            // 5.指定消息延迟等级为3级，即延迟10s
            message.setDelayTimeLevel(3);
            // 6.消息发送
            SendResult sendResult = producer.send(message);
            // 7.输出消息被发送的时间
            System.out.print(new SimpleDateFormat("mm:ss").format(new Date()));
            System.out.println(" ," + sendResult);
        }
        // 8.关闭生产者
        producer.shutdown();
    }
}
