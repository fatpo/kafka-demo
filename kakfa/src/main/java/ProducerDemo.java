import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {
    public static final String brokerList = "1.116.156.79:9092";
    public static final String topic = "test2";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);


        System.out.println("send a message: hello, Kafka!");
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        long time = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, time + "hello, Kafka! " + i);
            try {
                System.out.println(record);
                producer.send(record);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}