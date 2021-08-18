import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerDemoV6Partitioner {
    public static final String brokerList = "1.116.156.79:9092,1.116.156.79:9093,1.116.156.79:9094";
    public static final String topic = "test2";

    public static Properties initConfig() {
        Properties properties = new Properties();
        // 诸如“key.serializer”、“max.request.size”、“interceptor.classes”之类的字符串经常由于人为因素而书写错误
        // kafka 帮我们提供了一些constant常量
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");

        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        // 拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ProducerInterceptorPrefix1.class.getName() + "," + ProducerInterceptorPrefix2.class.getName());
        // 分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();

        System.out.println("send a message: hello, Kafka!");
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, "hello, Kafka!");
        // callback方式
        try {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("[send callback -1] topic: " + metadata.topic() + ", partition: " +
                                metadata.partition() + ", offset: " + metadata.offset());
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        // future方式
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println("[send callback -2] topic: " + metadata.topic() + ", partition: " +
                    metadata.partition() + ", offset: " + metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}