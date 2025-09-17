package com.cn.kafka.test.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author LuoXuanwei
 * @version 1.0
 * @description 自定义分区器
 * @date 2025/9/17 21:49
 *
 * 1、实现Partitioner接口
 * 2、重写方法
 */
public class MyKafkaPartPartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        // 实际上是按照业务逻辑来定返回的分区编号
        // 返回的分区器不能是负数或大于分区数
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
