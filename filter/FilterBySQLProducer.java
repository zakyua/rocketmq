package com.atguigu.rocketmq.filter;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * SQL过滤Producer
 * @author ChenCheng
 * @create 2022-06-21 16:46
 */
public class FilterBySQLProducer {
    public static void main(String[] args) throws MQClientException {
        // 1.创建一个Producer，参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 2.指定nameServer的地址
        producer.setNamesrvAddr("192.168.13.128:9876");
        // 3.开启生产者
        producer.start();

        // 4.生产消息
        for (int i = 0; i < 10; i++) {
            try {
                byte[] body = ("Hi," + i).getBytes();
                Message msg = new Message("FilterBySQLTopic", "FilterBySQLTag", body);
                // 定义将来需要过滤的条件
                msg.putUserProperty("age", i + "");
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}
