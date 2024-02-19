import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class Worker {
    static class WordCountProcessor implements Processor<String, Integer, String, Integer> {
        private KeyValueStore<String, Integer> kvStore;
        private ProcessorContext<String, Integer> context;
        final String applicationCounter = System.getenv("APPLICATION_COUNTER");

        @Override
        public void init(final ProcessorContext<String, Integer> context) {
            this.context = context;
            kvStore = context.getStateStore("Counts");
        }

        @Override
        public void process(final Record<String, Integer> record) {
            final Integer recordValue = record.value();
            final Integer oldInt = kvStore.get(record.key());
            final int old = Objects.requireNonNullElse(oldInt, 0);

            if (recordValue != old + 1) {
                System.err.println("[" + applicationCounter + "]" + "!!! BROKEN !!! Expected " + (recordValue - 1) + " but found " + old + " partition: " + context.recordMetadata().map(RecordMetadata::partition).orElseGet(() -> -1));
                throw new RuntimeException("Broken!");
            }


            kvStore.put(record.key(), record.value());
            context.forward(record);
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

        File dir = new File("state-dir-" + appId + "/" + System.getenv("APPLICATION_INSTANCE"));
        dir.mkdirs();
        System.out.println("State dir: " + dir.getAbsolutePath());

        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.putIfAbsent(StreamsConfig.STATE_DIR_CONFIG, dir.getAbsolutePath());
        props.putIfAbsent(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9085");
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.putIfAbsent(StreamsConfig.CLIENT_ID_CONFIG, "app-" + System.getenv("APPLICATION_INSTANCE"));


        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create 1 standby replica
        props.putIfAbsent(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);

        final Topology builder = new Topology();

        builder.addSource("Source", appId + "-input-topic");

        builder.addProcessor("Process", WordCountProcessor::new, "Source");


        builder.addStateStore(Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("Counts"),
                                Serdes.String(),
                                Serdes.Integer()).withCachingEnabled()
                        .withLoggingEnabled(new HashMap<String, String>() {{
                            put("segment.bytes", "20480");
                        }}),
                "Process");

        builder.addSink("Sink", appId + "-output-topic", "Process");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // add a global state restore listener
        streams.setGlobalStateRestoreListener(new StateRestoreListener() {
            @Override
            public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
                System.out.println("Restoring: start " + storeName + " from " + startingOffset + " to " + endingOffset);
            }

            @Override
            public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
                System.out.println("Restori ng: batch " + numRestored + " records to " + storeName + " at offset " + batchEndOffset);
            }

            @Override
            public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
                System.out.println("Restoring: end " + totalRestored + " records to " + storeName);
            }
        });

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close(new KafkaStreams.CloseOptions().leaveGroup(true));
                System.err.println("Streams closed");
                latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.out.println("ERROR DYING");
            System.exit(1);
        }
        System.exit(0);
    }
}
