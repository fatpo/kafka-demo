import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsumerCommitSyncBatch {
    public static final String brokerList = "1.116.156.79:9092,1.116.156.79:9093,1.116.156.79:9094";
    public static final String topic = "test2";
    public static final String groupId = "group.demo";


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
        //循环消费消息
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            List<ConsumerRecord<String, String>> buffer = new ArrayList<>(200);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                buffer.add(record);
            }
            if (buffer.size() >= 200) {
                consumer.commitAsync();
                buffer.clear();
            }

        }
    }
}