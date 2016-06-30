package com.rtmap.example.kafka.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * CommonProducer
 *
 * @author Muarine<maoyun@rtmap.com>
 * @date 2016 16/6/15 10:47
 * @since 0.1
 */
public class CommonProducer {

    public static void main(String[] args) {

//        Properties props = new Properties();
//        props.put("bootstrap.servers", "10.10.10.114:9091");
//        // 提交模式,all:记录全部提交
////        props.put("acks", "all");
//        // 重试次数
//        props.put("retries", 0);
//        // 维护未提交消息缓冲区的大小
//        props.put("batch.size", 16384);
//        // 单位时间内请求次数
//        props.put("linger.ms", 1);
//        // 发送和提交事务使用的总内存大小
//        props.put("buffer.memory", 33554432);
//        // 序列化
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//        Producer<String, String> producer = new KafkaProducer<>(props);
//        Integer partition = 1;
        String topic = "RT-KAFKA-SINK";
//        for(int i = 0; i < 1; i++)
//            producer.send(new ProducerRecord<>(topic , "key"+i , "message" + i));
//        System.out.println("发送完成");
//        producer.close();

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.10.10.114:9091,10.10.10.114:9092,10.10.10.114:9093");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 1; i++)
            producer.send(new ProducerRecord<>(topic , "KEY1111111", "我是消息内容,编号222222"));
        System.out.println("发送完成");
        producer.close();
        System.exit(0);


    }



}
