package com.cn.kafka.test.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * @author LuoXuanwei
 * @version 1.0
 * @description kafka生产者测试
 * @date 2025/9/17 22:02
 */
public class KafkaProducerCallBackTest {
    public static void main(String[] args) throws  Exception {
        // 创建配置对象
        Map<String, Object> configMap = new HashMap<>();
        // kafka集群地址
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // 对生产的数据K , V进行序列化的操作
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建生产者对象(生产者对象需要设定泛型:数据的类型约束)
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configMap);

        // 构建数据:需要传递三个参数
        // topic:主题(主题不存在则自动创建)
        // key:数据对应的key
        // value:数据对应的value
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> recode = new ProducerRecord<String, String>(
                    "test", "key" + i, "hello kafka " + i
            );

            // 通过生产者对象将数据发送到kafka(主线程负责发送数据,回调函数(下游线程)负责接收发送结果)
            Future<RecordMetadata> send = producer.send(recode, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("数据发送成:" + recordMetadata);
                }
            });

            System.out.println("发送数据中...");

            // 同步发送(不使用下面这个方式默认异步发送，异步效率更高)
            send.get();
        }

        // 关闭生产者对象
        producer.close();
    }
}
