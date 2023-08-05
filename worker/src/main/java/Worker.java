import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import os.temp;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.streams.processor.PunctuationType;

public final class Worker {
    static class WordCountProcessor implements Processor<String, Integer, String, Integer> {
        private KeyValueStore<String, Integer> kvStore;
        private ProcessorContext<String, Integer> context;
        final String instanceId = System.getenv("INSTANCE_ID");

        @Override
        public void init(final ProcessorContext<String, Integer> context) {
            this.context = context;
            kvStore = context.getStateStore("Counts");
            this.context.schedule(
                    Duration.ofSeconds(60),
                    PunctuationType.WALL_CLOCK_TIME, this::forwardAll);
        }

        @Override
        public void process(final Record<String, Integer> record) {
            final Integer recordValue = record.value();
            final Integer oldInt = kvStore.get(record.key());
            final int old = Objects.requireNonNullElse(oldInt, recordValue - 1);
            kvStore.put(record.key(), old + 1);
        }

        private void forwardAll(final long timestamp) {
            System.err.println("forwardAll Start");
            System.out.println("forwardAll Start");

            KeyValueIterator<String, Integer> kvList = this.kvStore.all();
            while (kvList.hasNext()) {
                KeyValue<String, Integer> entry = kvList.next();
                final Record<String, Integer> msg = new Record<>(entry.key, entry.value, context.currentSystemTimeMs());
                final Integer storeValue = this.kvStore.get(entry.key);

                if (entry.value != storeValue) {
                    System.err.println("[" + instanceId + "]" + "!!! BROKEN !!! Key: " + entry.key + " Expected in stored(Cache or Store) value: " + storeValue + " but KeyValueIterator value: " + entry.value);
                    System.out.println("[" + instanceId + "]" + "!!! BROKEN !!! Key: " + entry.key + " Expected in stored(Cache or Store) value: " + storeValue + " but KeyValueIterator value: " + entry.value);
                    throw new RuntimeException("Broken!");
                }

                this.context.forward(msg);
                // evict() call in delete.
                this.kvStore.delete(entry.key);
            }
            kvList.close();
            System.err.println("forwardAll end");
            System.out.println("forwardAll end");
        }

        @Override
        public void close() {
            // close any resources managed by this processor
            // Note: Do not close any StateStores as these are managed by the library
        }
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }

        final String appId = System.getenv("APPLICATION_ID");

        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.putIfAbsent(StreamsConfig.STATE_DIR_CONFIG, os.temp.dir(null, null, true, null).toString());
        props.putIfAbsent(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 6);
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 201326592);

        props.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.putIfAbsent(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");
        props.putIfAbsent(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 30000);
        props.putIfAbsent(StreamsConfig.POLL_MS_CONFIG, 1000);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.putIfAbsent(StreamsConfig.RECEIVE_BUFFER_CONFIG, 4194304);
        props.putIfAbsent(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 1800000L);
        props.putIfAbsent(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 0L);
        props.putIfAbsent(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, 500);
        props.putIfAbsent(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 1500000L);

        props.putIfAbsent(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 10000);
        props.putIfAbsent(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), 1000);
        props.putIfAbsent(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MIN_BYTES_CONFIG), 1);
        props.putIfAbsent(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 180000);
        props.putIfAbsent(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 600000);
        props.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        props.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);
        props.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 5);
        props.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), Integer.MAX_VALUE);
        props.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 600000);
        props.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), 300000);
        props.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 100);
        props.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 600000);
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());

        final Topology builder = new Topology();

        builder.addSource("Source", appId + "-input-topic");

        builder.addProcessor("Process", WordCountProcessor::new, "Source");

        Map<String, String> loggingConfig = new HashMap<>();
        loggingConfig.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
        loggingConfig.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(600000));
        loggingConfig.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, Long.toString(600000));
        loggingConfig.put(TopicConfig.SEGMENT_BYTES_CONFIG, Long.toString(10485760));
        loggingConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        loggingConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Long.toString(1));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("Counts"),
                                Serdes.String(),
                                Serdes.Integer()).withCachingEnabled()
                        .withLoggingEnabled(loggingConfig),
                "Process");

        builder.addSink("Sink", appId + "-output-topic", "Process");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        streams.setUncaughtExceptionHandler(ex -> {
            System.err.println("ERROR DYING");
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(10));
                System.err.println("Streams closed");
                latch.countDown();
            }
        });
        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.out.println("ERROR DYING");
            System.exit(1);
        }
        System.exit(0);
    }
}
