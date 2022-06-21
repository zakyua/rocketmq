package com.atguigu.rocketmq.transaction;

import org.apache.commons.validator.routines.checkdigit.IBANCheckDigit;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.*;

/**
 * 事务消息生产者
 * @author ChenCheng
 * @create 2022-06-21 15:35
 */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 1.创建一个事务的Producer，参数为Producer Group名称
        TransactionMQProducer producer = new TransactionMQProducer("tpg");
        // 2.指定nameServer的地址
        producer.setNamesrvAddr("192.168.13.128:9876");

        // 3.定义一个线城池
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        // 4.为线程指定一个线程池
        producer.setExecutorService(executorService);
        // 5.为生产者添加事务监听器
        producer.setTransactionListener(new ICBCTransactionListener());
        // 6.开启生产者
        producer.start();

        String[] tags = {"TAGA","TAGB","TAGC"};
        for (int i = 0; i < 3; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("TransactionTopic", tags[i], body);
            // 发送事务消息
            // 第二个参数用于指定在执行本地事务时要使用的业务参数
            SendResult sendResult = producer.sendMessageInTransaction(msg,null);
            System.out.println("发送结果为：" + sendResult.getSendStatus());
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }
}
