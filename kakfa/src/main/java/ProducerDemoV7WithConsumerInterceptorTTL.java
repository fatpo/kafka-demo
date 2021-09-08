import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoV7WithConsumerInterceptorTTL {
    public static final String brokerList = "1.116.156.79:9092";
    public static final String topic = "test2";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);


        System.out.println("send a message: hello, Kafka!");
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        ProducerRecord<String, String> record1 =
                new ProducerRecord<>(topic, 0, System.currentTimeMillis() - 11 * 1000, null, "record 1");
        producer.send(record1).get();

        ProducerRecord<String, String> record2 =
                new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, "record 2");
        producer.send(record2).get();

        ProducerRecord<String, String> record3 =
                new ProducerRecord<>(topic, 0, System.currentTimeMillis() - 11 * 1000, null, "record 3");
        producer.send(record3).get();


        producer.close();
    }
}