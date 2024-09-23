package org.example.globalstate;

/**
 * Opportunity here to support serdes' from lots of other frameworks via composition. Just allow setting different
 * serializers, marhsallers, or other types of things that can serve as key and/or value parsers.
 */
public interface MessageParser {
    KeyValue parse(KeyValue keyValue);
}
