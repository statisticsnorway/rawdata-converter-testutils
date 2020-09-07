package no.ssb.rawdata.converter.test.message;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RawdataMessageFixtures {

    private Map<String, RawdataMessages> rawdataMessagesByGroup = new HashMap<>();

    public static RawdataMessageFixtures init(String... groups) {
        return init(Paths.get("src/test/resources/rawdata-messages"), groups);
    }

    public static RawdataMessageFixtures init(Path rootPath, String... groups) {
        RawdataMessageFixtures fixtures = new RawdataMessageFixtures();
        try {
            for (String group : groups) {
                fixtures.rawdataMessagesByGroup.put(group, loadRawdataMessages(rootPath, group));
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fixtures;
    }

    static RawdataMessages loadRawdataMessages(Path rootPath, String group) throws IOException {
        final AtomicLong sequenceNo = new AtomicLong();
        Map<String, RawdataMessage> messagesMap = new LinkedHashMap<>();
        final ULID ulid = new ULID();

        Files.list(rootPath.resolve(group)).sorted().filter(Files::isDirectory).forEach(path -> {
            String position = path.getFileName().toString();

            try {
                Map<String, byte[]> data = Files.list(path)
                  .collect(Collectors.toMap(
                    p -> p.getFileName().toString(),
                    p -> readAllBytes(p)
                  ));

                messagesMap.put(position, new MemoryRawdataMessage(ulid.nextValue(), group, sequenceNo.incrementAndGet(), position, data));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return new RawdataMessages(messagesMap);
    }

    static byte[] readAllBytes(Path p) {
        try {
            return Files.readAllBytes(p);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public RawdataMessages rawdataMessages(String group) {
        return Optional.ofNullable(rawdataMessagesByGroup.get(group))
          .orElseThrow(() -> new IllegalArgumentException("Unknown rawdata message group: " + group));
    }

}
