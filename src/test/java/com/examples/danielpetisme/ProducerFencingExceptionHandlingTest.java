package com.examples.danielpetisme;

import io.github.netmikey.logunit.api.LogCapturer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@Testcontainers
public class ProducerFencingExceptionHandlingTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(ProducerFencingExceptionHandlingTest.class);

    public static final String CLIENT_ID = ProducerFencingExceptionHandlingTest.class.getName();
    static final String INPUT_TOPIC = "in";

    static final String OUTPUT_TOPIC = "out";

    @RegisterExtension
    LogCapturer logs = LogCapturer.create().captureForType(ProducerFencingExceptionHandlingTest.class);

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));
    public KafkaStreams streams;

    Topology createTopology() {
        final var builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> LOGGER.info("Read record: " + k + " => " + v))
                .to(OUTPUT_TOPIC,
                        Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    @AfterEach
    public void afterEach() {
        streams.close(Duration.ofSeconds(3));
    }

    @Test
    public void test() throws Exception {
        createTopics(List.of(INPUT_TOPIC, OUTPUT_TOPIC), 1, 1);

        MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaClientsSupplierWithCustomerProducer kafkaClientsSupplier = new KafkaClientsSupplierWithCustomerProducer(mockProducer);
        mockProducer.initTransactions();
        mockProducer.fenceProducer();

        Properties streamProperties = new Properties();
        streamProperties.putAll(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                StreamsConfig.APPLICATION_ID_CONFIG, "test",
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2,
                StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, CustomProductionExceptionHandler.class,
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest"
        ));

        streams = new KafkaStreams(createTopology(), streamProperties, kafkaClientsSupplier);
        streams.setUncaughtExceptionHandler((exception) -> {
            LOGGER.error("Uncaught exception: {}", exception.getMessage());
            return SHUTDOWN_APPLICATION;
        });
        streams.start();

        Map<String, Object> inputTopicProducerConfig = Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                CLIENT_ID, "input-topic-producer"
        );

        try (KafkaProducer<String, String> inputTopicProducer = new KafkaProducer<String, String>(inputTopicProducerConfig, new StringSerializer(), new StringSerializer())) {
            inputTopicProducer.send(new ProducerRecord<>(INPUT_TOPIC, "key", "value")).get();
        } catch (Exception e) {
            fail();
        }
        Thread.sleep(1000);
        logs.assertContains("Uncaught exception: Error encountered trying to initialize transactions [stream-thread [main]]");
        logs.assertDoesNotContain("Production exception when producing record:");
        assertThat(streams.state()).isEqualTo(KafkaStreams.State.ERROR);
    }

    private static void createTopics(List<String> topics, int partitions, int rf) throws InterruptedException, ExecutionException {
        var adminClient = AdminClient.create(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ));

        // Create topic with 3 partitions
        for (String topicName : topics) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                LOGGER.info("Creating topic {}", topicName);
                final NewTopic newTopic = new NewTopic(topicName, partitions, (short) rf);
                try {
                    CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                    topicsCreationResult.all().get();
                } catch (Exception e) {
                    //silent ignore if topic already exists
                }
            }
        }
    }

    public class KafkaClientsSupplierWithCustomerProducer extends DefaultKafkaClientSupplier {

        final Producer<byte[], byte[]> producer;

        public KafkaClientsSupplierWithCustomerProducer(Producer<byte[], byte[]> producer) {
            this.producer = producer;
        }

        @Override
        public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
            return producer;
        }
    }

    public static class CustomProductionExceptionHandler implements ProductionExceptionHandler {

        public CustomProductionExceptionHandler() {
            //NO-OPS
        }

        @Override
        public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
            LOGGER.error("Production exception when producing record: {} caused by: {}", record, exception);
            return ProductionExceptionHandlerResponse.FAIL;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            //NO-OPS
        }
    }

}
