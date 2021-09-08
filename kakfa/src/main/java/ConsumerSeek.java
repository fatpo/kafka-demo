import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class ConsumerSeek {
    public static final String brokerList = "1.116.156.79:9092,1.116.156.79:9093,1.116.156.79:9094";
    public static final String topic = "test2";
    public static final String groupId = "group.demo-1";


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", brokerList);
        //设置消费组的名称，具体的释义可以参见第3章
        properties.put("group.id", groupId);
        //创建一个消费者客户端实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));

        // 获取分区数据
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(1000));
            assignment = consumer.assignment();
            System.out.println("assignment: " + assignment);
        }

        // 重新seek 到 0
        for (TopicPartition partition : assignment) {
            System.out.println("seek: " + partition + ", offset: 0");
            consumer.seek(partition, 0);
        }

        //循环消费消息
        while (true) {
            System.out.println("##########");
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
            consumer.commitAsync();
        }
    }
}