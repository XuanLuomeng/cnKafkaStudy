package com.cn.kafka.test.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author LuoXuanwei
 * @version 1.0
 * @description TODU
 * @date 2025/9/17 20:39
 */
public class AdminTopicTest {
    public static void main(String[] args) {
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");

        // 管理员对象
        Admin admin = Admin.create(confMap);

        // 构建主题
        // 主题1名称
        String topicName = "topic-test1";
        // 分区1数量
        int partitionCount = 1;
        // 副本1数量
        short replicationCount = 1;
        NewTopic newTopic1 = new NewTopic(topicName, partitionCount, replicationCount);

        // 构建主题2
        String topicName2 = "topic-test2";
        int partitionCount2 = 2;
        short replicationCount2 = 2;
        NewTopic newTopic2 = new NewTopic(topicName2, partitionCount2, replicationCount2);

        // 构建主题3
        // 自动分区
        String topicName3 = "topic-test3";
        Map<Integer, List<Integer>> map = new HashMap<>();
        // key        leader/follower
        map.put(0, Arrays.asList(3,1));
        map.put(1, Arrays.asList(2,2));
        map.put(2, Arrays.asList(1,3));
        NewTopic newTopic3 = new NewTopic(topicName3, map);

        // 创建主题
        CreateTopicsResult topics = admin.createTopics(
                Arrays.asList(newTopic1, newTopic2, newTopic3)
        );

        // 关闭管理者对象
        admin.close();
    }
}
