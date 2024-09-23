package org.example.globalstate;

public abstract class AbstractNullValuePassingMessageParser implements MessageParser {
    @Override
    public KeyValue parse(KeyValue keyValue) {
        Object inputValue = keyValue.value();
        return new KeyValue(
                parseKey(keyValue.key()),
                inputValue == null ? null : parseValue(inputValue)
        );
    }

    public abstract Object parseKey(Object key);
    public abstract Object parseValue(Object value);

}
