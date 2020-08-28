package no.ssb.rawdata.converter.test;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RawdataMessageFixtures {

    private Map<String, RawdataMessage> altinn3Messages;
    private Map<String, RawdataMessage> fregMessages;
    private Map<String, RawdataMessage> bongNgMessages;

    static byte[] readAllBytes(Path p) {
        try {
            return Files.readAllBytes(p);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Map<String, RawdataMessage> loadRawdataMessages(String group) throws IOException {
        final AtomicLong sequenceNo = new AtomicLong();
        Map<String, RawdataMessage> messages = new HashMap<>();
        final ULID ulid = new ULID();

        Files.list(Paths.get("src/test/resources/rawdata/" + group)).sorted().filter(Files::isDirectory).forEach(path -> {
            String position = path.getFileName().toString();

            try {
                Map<String, byte[]> data = Files.list(path)
                  .collect(Collectors.toMap(
                    p -> p.getFileName().toString(),
                    p -> readAllBytes(p)
                  ));

                messages.put(position, new MemoryRawdataMessage(ulid.nextValue(), group, sequenceNo.incrementAndGet(), position, data));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return messages;
    }

    public static RawdataMessageFixtures init() {
        RawdataMessageFixtures fixtures = new RawdataMessageFixtures();
        try {
            fixtures.altinn3Messages = loadRawdataMessages("altinn3");
            fixtures.fregMessages = loadRawdataMessages("freg");
            fixtures.bongNgMessages = loadRawdataMessages("bong-ng");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fixtures;
    }

    public Map<String, RawdataMessage> altinn3Messages() {
        return altinn3Messages;
    }

    public Map<String, RawdataMessage> fregMessages() {
        return fregMessages;
    }

    public Map<String, RawdataMessage> bongNgMessages() {
        return bongNgMessages;
    }

}
