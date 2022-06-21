package com.atguigu.rocketmq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * 顺序消息
 * 顺序消息指的是，严格按照消息的发送顺序进行消费的消息(FIFO)。
 * @author ChenCheng
 * @create 2022-06-21 14:36
 */
public class OrderedProducer {


    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 1.创建一个Producer，参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 2.指定nameServer的地址
        producer.setNamesrvAddr("192.168.13.128:9876");

        // 若为全局有序，则需要设置Queue数量为1
        // producer.setDefaultTopicQueueNums(1);
        // 3.开启生产者
        producer.start();

        for (int i = 0; i < 100; i++) {
            // 4.为了演示简单，使用整型数作为orderId
            Integer orderId = i;
            byte[] body = ("Hi," + i).getBytes();
            Message message = new Message("OrderedTopic", "OrderedTag", body);
            // 5.将orderId作为消息key
            message.setKeys(orderId.toString());

            // send()的第三个参数值会传递给选择器的select()的第三个参数
            // 该send()为同步发送
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(
                        List<MessageQueue> mqs,
                        Message message,
                        Object o) {
                    // 以下是使用消息key作为选择的选择算法
                    String keys = message.getKeys();
                    Integer id = Integer.valueOf(keys);

                    // 以下是使用arg作为选择key的选择算法
                    // Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);
            System.out.println(sendResult);
        }
        producer.shutdown();


    }


}
