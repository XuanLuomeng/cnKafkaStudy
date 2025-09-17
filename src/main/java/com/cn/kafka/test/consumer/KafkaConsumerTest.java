package com.cn.kafka.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author LuoXuanwei
 * @version 1.0
 * @description kafka消费者测试
 * @date 2025/9/17 16:37
 */
public class KafkaConsumerTest {
    public static void main(String[] args) {
        // 创建消费者配置对象
        Map<String, Object> consumerConfig = new HashMap<>();
        // kafka集群地址
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // key value 反序列化器
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 消费者组(关键点，没有则无法获取到数据)
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");

        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig);

        // 订阅主题
        consumer.subscribe(Collections.singletonList("test"));

        // 从kafka的主题中获取数据(主动拉取数据)
        while(true) {
            ConsumerRecords<String, String> datas = consumer.poll(100);

            System.out.println("从kafka主题中获取数据条数：" + datas.count());
            System.out.println("从kafka主题中获取数据：");
            for (ConsumerRecord<String, String> data : datas) {
                System.out.println(data.value());
            }
        }

        // 关闭消费者对象
//        consumer.close();
    }
}
