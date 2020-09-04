# Rawdata Converter Testutils

Misc support utilities for rawdata converter testing.

## Examples

#### Loading Rawdata Messages from file

The following snippet shows how you could load rawdata message fixtures from local disk:

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
