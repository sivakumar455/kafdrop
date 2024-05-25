package kafdrop.service;

import jakarta.annotation.PostConstruct;
import kafdrop.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

@Service
public final class KafkaHighLevelProducer {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaHighLevelProducer.class);
  private Producer<String, String> producer;
  private final KafkaConfiguration kafkaConfiguration;

  public KafkaHighLevelProducer(KafkaConfiguration kafkaConfiguration) {
    this.kafkaConfiguration = kafkaConfiguration;
  }

  @PostConstruct
  private void initializeClient() {
    if (producer == null) {
      final var properties = new Properties();
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafdrop-producer");
      kafkaConfiguration.applyCommon(properties);
      LOG.debug("Producer Config Properties: {}", properties);

      producer = new KafkaProducer<>(properties);
    }
  }

  private void addOptionalHeaders(RecordHeaders recordHeaders, String headers) {
    if (headers != null && !headers.isEmpty()) {
      String[] keyValuePairs = headers.split(",");
      for (String pair : keyValuePairs) {
        String[] elements = pair.split("=");
        if (elements.length == 2) {
          String key = elements[0].trim();
          String value = elements[1].trim();
          recordHeaders.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
        }
      }
    }
  }

  public void publish(String producerTopic, String payload, String publishOptionalHeaders,
                                   String optionalHeaders) {
    String key = "key123";
    RecordHeaders recordHeaders = new RecordHeaders();
    if (publishOptionalHeaders != null && publishOptionalHeaders.equalsIgnoreCase("true")) {
      addOptionalHeaders(recordHeaders, optionalHeaders);
    }
    try {
      synchronized(this) {
        producer.send(new ProducerRecord<>(producerTopic, 0, key, payload, recordHeaders));
      }
      LOG.info("Published successfully on topic: {}", producerTopic );
    } catch (Exception ex) {
      LOG.error("Error published on topic: {} with errMsg: {}", producerTopic, ex.getMessage() );
      throw ex;
    }
  }
}
