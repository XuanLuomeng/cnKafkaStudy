package com.cn.kafka.test.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author LuoXuanwei
 * @version 1.0
 * @description 自定义拦截器测试
 * @date 2025/9/17 21:31
 * <p>
 * 1、实现ProducerInterceptor接口
 * 2、定义泛型
 * 3、重写方法
 */
public class ValueInterceptorTest implements ProducerInterceptor<String, String> {
    @Override
    // 发送消息之前执行
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<String, String>(
                producerRecord.topic(),
                producerRecord.key(),
                producerRecord.value() + "---"
        );
    }

    @Override
    // 发送消息,服务器返回的响应之后执行
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    // 生产者对象关闭的时候，会调用此方法
    public void close() {

    }

    @Override
    // 创建生产者对象的时候，会调用此方法
    public void configure(Map<String, ?> map) {

    }
}
