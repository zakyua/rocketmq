package com.atguigu.rocketmq.general;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 异步消息发送生产者
 * @author ChenCheng
 * @create 2022-06-21 13:52
 */
public class AsyncProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 1.创建一个Producer，参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 2.指定nameServer的地址
        producer.setNamesrvAddr("192.168.13.128:9876");
        // 3.设置异步发送失败后不进行重试发送
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 4.设置新创建的Topic的Queue数量为2，默认为4
        producer.setDefaultTopicQueueNums(2);

        // 5.开启生产者
        producer.start();

        // 6.生产100条信息
        for (int i = 1; i <= 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            try {
                Message message = new Message("AsyncTopic", "AsyncTag", body);
                // 7.异步发送消息,没有返回值，需要指定回调
                producer.send(message, new SendCallback() {
                    // 异步发送，当producer接收到MQ发送来的ACK后就会触发该回调方法的执行
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult);
                    }

                    // 出现异常
                    @Override
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // sleep一会儿
        // 由于采用的是异步发送，所以若这里不sleep，
        // 则消息还未发送就会将producer给关闭，报错
        TimeUnit.SECONDS.sleep(3);

        // 9.关闭producer
        producer.shutdown();





     }




}


