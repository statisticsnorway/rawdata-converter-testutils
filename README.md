[![Build Status](https://dev.azure.com/statisticsnorway/Dapla/_apis/build/status/statisticsnorway.rawdata-converter-testutils?repoName=statisticsnorway%2Frawdata-converter-testutils&branchName=master)](https://dev.azure.com/statisticsnorway/Dapla/_build/latest?definitionId=95&repoName=statisticsnorway%2Frawdata-converter-testutils&branchName=master)
# Rawdata Converter Testutils

Misc support utilities for rawdata converter testing.

## Maven
```xml
    <dependency>
      <groupId>no.ssb.rawdata.converter</groupId>
      <artifactId>rawdata-converter-testutils</artifactId>
      <version>x.x.x</version>
      <scope>test</scope>
    </dependency>
```

## Examples

### Loading Rawdata Messages from file

```java
import no.ssb.rawdata.converter.core.util.RawdataMessageFacade;
import no.ssb.rawdata.converter.test.message.RawdataMessageFixtures;
import no.ssb.rawdata.converter.test.message.RawdataMessages;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SomeRawdataMessageTest {

    static RawdataMessageFixtures fixtures;

    @BeforeAll
    static void loadFixtures() {
        fixtures = RawdataMessageFixtures.init("topic-1", "topic-2");
    }

    @Test
    void testStuff() {
        RawdataMessages messages = fixtures.rawdataMessages("topic-1");
        RawdataMessageFacade.print(messages.index().get("position-1"));
    }
}
```

The location of the files can be specified explicitly, or is assumed to be found at `/src/test/resources/rawdata-messages`.
Here's an example of an expected directory structure:
```
src/test/resources/
└── rawdata-messages
    └── topic-1
    │   ├── position-1
    │   │   └── entry
    │   │   └── manifest.json
    │   └── position-2
    │       └── entry
    │       └── manifest.json
    └── topic-2
        └── 123
            └── entry
            └── manifest.json
```

### Create a rawdata source using `RawdataMessageFixtures` and `RawdataPublisher`

```java
import static no.ssb.rawdata.converter.test.RawdataPublisher.filesystemConfig;
import static no.ssb.rawdata.converter.test.RawdataPublisher.postgresConfig;
import static no.ssb.rawdata.converter.test.RawdataPublisher.publishRawdataMessages;

public class SomeRawdataPublisherTest {

    static RawdataMessageFixtures fixtures;

    @BeforeAll
    static void loadFixtures() {
        fixtures = RawdataMessageFixtures.init("topic-1");
    }

    @Test
    @Disabled
    void publishRawdataMessagesToLocalPostgres() {
        String topic = "topic-1";
        RawdataMessages messages = fixtures.rawdataMessages(topic);
        publishRawdataMessages(messages, postgresConfig(topic));
    }

    @Test
    @Disabled
    void publishRawdataMessagesToLocalAvroFile() {
        String topic = "topic-1";
        RawdataMessages messages = fixtures.rawdataMessages(topic);
        publishRawdataMessages(messages, filesystemConfig(topic, "./rawdatastore")
          .cleanupBefore(true) // remove files from previous runs
          .cleanupAfter(true) // remove temp files
        );
    }
}
```

## Development

### Make targets
You can use `make` to execute common tasks:
```
build                          Build the project and install to you local maven repo
release-dryrun                 Simulate a release in order to detect any issues
release                        Release a new version. Update POMs and tag the new version in git. Pipeline will deploy upon tag detection.
```
