package com.atguigu.rocketmq.filter;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Tag过滤Producer
 * @author ChenCheng
 * @create 2022-06-21 16:32
 */
public class FilterByTagProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 1.创建一个Producer，参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 2.指定nameServer的地址
        producer.setNamesrvAddr("192.168.13.128:9876");
        // 3.开启生产者
        producer.start();

        // 4.指定tags
        String[] tags = {"myTagA","myTagB","myTagC"};
        // 5.生产10条信息
        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hi," + i).getBytes();
            String tag = tags[i%tags.length];
            Message msg = new Message("FilterTopic",tag,body);
            // 8.同步发送消息,同步发送消息会有返回值
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);
        }
        // 9.关闭producer
        producer.shutdown();
    }
}
