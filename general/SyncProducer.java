package com.atguigu.rocketmq.general;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;


/**
 *
 *  发送同步消息
 * @author ChenCheng
 * @create 2022-06-21 13:35
 */
public class SyncProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 1.创建一个Producer，参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 2.指定nameServer的地址
        producer.setNamesrvAddr("192.168.13.128:9876");
        // 3.设置当发送失败的时重试发送的次数，默认是两次
        producer.setRetryTimesWhenSendFailed(3);
        // 4.设置超时时间为5秒，默认是3秒
        producer.setSendMsgTimeout(5000);

        // 5.开启生产者
        producer.start();

        // 6.生产100条信息
        for (int i = 1; i <= 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message message = new Message("SyncTopic", "SyncTag", body);
            // 7.为消息指定key
            message.setKeys("key_"+i);
            // 8.同步发送消息
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        // 9.关闭producer
        producer.shutdown();
    }

}
