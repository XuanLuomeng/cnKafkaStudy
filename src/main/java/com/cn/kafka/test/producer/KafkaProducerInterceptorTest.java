package com.cn.kafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author LuoXuanwei
 * @version 1.0
 * @description kafka生产者拦截器测试
 * @date 2025/9/17 21:47
 */
public class KafkaProducerInterceptorTest {
    public static void main(String[] args) {
        // 创建配置对象
        Map<String, Object> configMap = new HashMap<>();
        // kafka集群地址
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // 对生产的数据K , V进行序列化的操作
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 添加自定义拦截器
        configMap.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ValueInterceptorTest.class.getName());

        // 创建生产者对象(生产者对象需要设定泛型:数据的类型约束)
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configMap);

        // 构建数据:需要传递三个参数
        // topic:主题(主题不存在则自动创建)
        // key:数据对应的key
        // value:数据对应的value
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> recode = new ProducerRecord<String, String>(
                    // 主题 键 值
//                    "test", "key" + i, "hello kafka " + i
                    // 主题 分区 键 值
                    "test",1 , "key" + i, "hello kafka " + i

            );

            // 通过生产者对象将数据发送到kafka
            producer.send(recode);
        }

        // 关闭生产者对象
        producer.close();
    }
}
