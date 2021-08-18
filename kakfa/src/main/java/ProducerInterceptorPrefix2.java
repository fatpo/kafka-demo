import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class ProducerInterceptorPrefix2 implements ProducerInterceptor<String, String> {

    private volatile Integer sendSuccess = 0;
    private volatile Integer sendFailure = 0;


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifyValue = "prefix2-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), modifyValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        System.out.println("interceptor2 send success:" + sendSuccess + ", send failure:" + sendFailure);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
