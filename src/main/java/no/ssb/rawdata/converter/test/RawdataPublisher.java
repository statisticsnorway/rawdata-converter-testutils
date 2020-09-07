package no.ssb.rawdata.converter.test;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.converter.test.message.RawdataMessages;
import no.ssb.service.provider.api.ProviderConfigurator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for publishing RawdataMessages to a rawdata store. You can use RawdataMessageFixtures to load rawdata
 * messages from file.
 *
 * This can be handy if you want to test a RawdataConverter or ConverterJobExecutor against "hand-made" rawdata messages.
 */
public class RawdataPublisher {

    /**
     * Publish a collection of RawdataMessages
     *
     * @param config rawdata client configuration
     * @param rawdataMessages rawdata messages, typically loaded using the RawdataMessageFixtures constructs
     */
    public static void publishRawdataMessages(RawdataMessages rawdataMessages, Config config) {
        new RawdataPublisher().publish(rawdataMessages, config);
    }

    /**
     * Publish a collection of RawdataMessages
     *
     * @param config rawdata client configuration
     * @param rawdataMessages rawdata messages, typically loaded using the RawdataMessageFixtures constructs
     */
    public void publish(RawdataMessages rawdataMessages, Config config) {
        RawdataClient client = createRawdataClient(config);
        RawdataProducer producer = client.producer(config.getTopic());
        List<String> positions = new ArrayList<>();

        rawdataMessages.index().forEach((position, msg) -> {
            positions.add(position);
            try {
                RawdataMessage.Builder msgBuilder = producer.builder().position(position);
                msg.keys().forEach(contentKey -> msgBuilder.put(contentKey, msg.get(contentKey)));
                producer.buffer(msgBuilder);
            }
            catch (Exception e) {
                throw new RuntimeException("Error buffering rawdata position " + position);
            }
        });

        onBeforePublish(config);
        producer.publish(positions);
        onAfterPublish(config);
    }

    /**
     * Invoked BEFORE publish
     * @param config
     */
    void onBeforePublish(Config config) {
        if (config.isFilesystemProvider()) {
            if (config.cleanupBefore) {
                deletePath(config.get("filesystem.storage-folder") + "/" + config.getTopic());
            }
        }
    }

    void onAfterPublish(Config config) {
        if (config.isFilesystemProvider()) {
            try {
                Thread.sleep(2000); // Wait to allow for filesystem rawdata client provider to finish
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (config.cleanupAfter()) {
                deletePath(config.get("local-temp-folder"));
            }
        }
    }

    /**
     * Instantiate RawdataClient
     */
    private RawdataClient createRawdataClient(Config config) {
        return ProviderConfigurator.configure(config, config.getRawdataClientProviderId(), RawdataClientInitializer.class);
    }

    /**
     * Delete a path and all its contained files
     *
     * @param pathToDelete path to directory or file to be deleted
     */
    private void deletePath(String pathToDelete) {
        Path path  = Path.of(pathToDelete);
        if (Files.exists(path)) {
            System.out.println("Delete folder " + path.toAbsolutePath());
            try {
                Files.walk(path)
                  .sorted(Comparator.reverseOrder())
                  .map(Path::toFile)
                  .forEach(File::delete);
            }
            catch (IOException e) {
                throw new RuntimeException("Error deleting " + path, e);
            }
        }
    }

    /**
     * Use this as baseline to publish rawdata messages to a local postgres db.
     *
     * You can spin up a postgres db by going to rawdata-converter-project/localenv
     * and running `make start-db`
     *
     * @param topic the topic name to publish to
     * @return a RawdataClientConfig for local postgres
     */
    public static Config postgresConfig(String topic) {
        Config config = Config.of(
          Map.of(
            "rawdata.client.provider", "postgres",
            "rawdata.client.topic", topic,
            "rawdata.postgres.consumer.prefetch-size", "100",
            "rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "100",
            "postgres.driver.host", "localhost",
            "postgres.driver.port", "15432",
            "postgres.driver.user", "rdc",
            "postgres.driver.password", "rdc",
            "postgres.driver.database", "rdc"
          )
        );
        return config;
    }

    /**
     * Use this as baseline to publish rawdata messages to local avro file(s).
     *
     * @param topic the topic name to publish to
     * @param storageFolder relative path to where the resulting avro file will be stored, e.g. ./rawdatastore
     * @return
     */
    public static Config filesystemConfig(String topic, String storageFolder) {
        Config config = Config.of(
          Map.of(
            "rawdata.client.provider", "filesystem",
            "rawdata.client.topic", topic,
            "filesystem.storage-folder", storageFolder,
            "local-temp-folder", "temp",
            "listing.min-interval-seconds", "0",
            "avro-file.max.seconds", "0", // Time to wait before flushing temp files to storage
            "avro-file.max.bytes", "10485760",
            "avro-file.sync.interval", "16"
          )
        );
        return config;
    }

    /**
     * Configuration map wrapper
     */
    public static class Config extends HashMap<String, String> {

        private boolean cleanupBefore = false;
        private boolean cleanupAfter = false;

        public static Config of(Map<String, String> config) {
            Config rawdataClientConfig = new Config();
            rawdataClientConfig.putAll(config);
            return rawdataClientConfig;
        }

        public String getRawdataClientProviderId() {
            return get("rawdata.client.provider");
        }

        public String getTopic() {
            return get("rawdata.client.topic");
        }

        public boolean isFilesystemProvider() {
            return "filesystem".equals(get("rawdata.client.provider"));
        }

        /** Set whether or not to perform cleanup before publishing (e.g. delete any previously generated targets) */
        public Config cleanupBefore(boolean cleanup) {
            this.cleanupBefore = cleanup;
            return this;
        }

        /** Set whether or not to perform cleanup afterwards */
        public Config cleanupAfter(boolean cleanup) {
            this.cleanupAfter = cleanup;
            return this;
        }

        /** whether or not to perform cleanup before publishing - defaults to false */
        public boolean cleanupBefore() {
            return cleanupBefore;
        }

        /** whether or not to perform cleanup afterwards - defaults to false */
        public boolean cleanupAfter() {
            return cleanupBefore;
        }
     }

}
