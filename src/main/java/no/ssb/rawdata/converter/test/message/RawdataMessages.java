package no.ssb.rawdata.converter.test.message;

import no.ssb.rawdata.api.RawdataMessage;

import java.util.Map;

public class RawdataMessages {
    private final Map<String, RawdataMessage> messagesMap;

    public RawdataMessages(Map<String, RawdataMessage> messagesMap) {
        this.messagesMap = messagesMap;
    }

    public Map<String, RawdataMessage> index() {
        return messagesMap;
    }
}
