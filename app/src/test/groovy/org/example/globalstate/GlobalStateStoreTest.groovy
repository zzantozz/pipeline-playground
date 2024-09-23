package org.example.globalstate

import com.fasterxml.jackson.databind.ObjectMapper;
import spock.lang.Specification

class GlobalStateStoreTest extends Specification  {
    final def mapper = new ObjectMapper()

    def "it works with no configuration and simply stores the raw keys and values, removing them when receiving a null value"() {
        given:
        def appMemory = [:]
        def store = new GlobalStateStore(appMemory)

        when:
        def key = "foo".bytes
        def value = "bar".bytes
        store.ingest(key, value)

        then:
        store.get(key) == value
        appMemory.get(key) == value

        when:
        store.ingest(key, null)

        then:
        !store.containsKey(key)
        !appMemory.containsKey(key)
    }

    def 'it lets you convert incoming keys and value to POJOs 1:1 with the raw values stored in the db and the POJOs stored in memory'() {
        given:
        def appMemory = [:]
        def store = new GlobalStateStore(appMemory)
        store.messageParser = new MessageParser() {
            KeyValue parse(KeyValue keyValue) {
                new KeyValue(
                        mapper.readValue(keyValue.key(), Map),
                        keyValue.value() == null ? null : mapper.readValue(keyValue.value(), Map),
                )
            }
        }

        when:
        def key = '{"hello": "world"}'.bytes
        def value = '{"name": "bob", "age": 42}'.bytes
        store.ingest(key, value)

        then:
        store.get(key) == value
        def expectedKeyMap = [hello: 'world']
        def expectedValueMap = [name: 'bob', age: 42]
        store.getParsed(key).key() == expectedKeyMap
        store.getParsed(key).value() == expectedValueMap
        appMemory.get(expectedKeyMap) == expectedValueMap

        when:
        // Note that this test assumes that keys in tombstone (null value) messages can be mapped to storage keys by the
        // user, without needing to store anything internally. In other words, the user must be able to provide a
        // storage key from a given message key entirely on their own.
        store.ingest(key, null)

        then:
        !store.containsKey(key)
        !appMemory.containsKey(expectedKeyMap)
    }

    def 'it lets you convert incoming messages into multiple POJOs stored separately'(){
        given:
        // Multiple app-side maps to manage data in
        def appPersonStorage = [:]
        def appDogStorage = [:]
        def topology = new AppMemoryTopology([
                people: appPersonStorage,
                dogs  : appDogStorage,
        ])
        def store = new GlobalStateStore(topology)
        // A message parser that decodes keys as strings and values as JSON
        store.messageParser = new MessageParser() {
            KeyValue parse(KeyValue keyValue) {
                new KeyValue(
                        new String(keyValue.key()),
                        keyValue.value() == null ? null : mapper.readValue(keyValue.value(), Map),
                )
            }
        }
        // A splitter that pulls nested objects from the value to store in separate regions
        store.messageSplitter = new MessageSplitter() {
            // Is this too much work to put on a user? It's basically asking them to handle null values and map only
            // keys or full key/values depending on what comes in. Perhaps it would be better overall to have separate
            // parseKey and parseValue methods everywhere, but it's possible one could need the other for context, so
            // we'd always need to pass the entire input KeyValue as context.
            List<StorageOperation> split(KeyValue keyValue) {
                if (keyValue.value() == null) {
                    return [
                            new RemoveStorageOperation('people', new KeyValue('aaa', null)),
                            new RemoveStorageOperation('dogs', new KeyValue('bbb', null)),
                    ]
                } else {
                    def personPojo = keyValue.value()['person']
                    def dogPojo = keyValue.value()['dog']
                    def personKv = new KeyValue('aaa', personPojo)
                    def dogKv = new KeyValue('bbb', dogPojo)
                    return [
                            new PutStorageOperation('people', personKv),
                            new PutStorageOperation('dogs', dogKv),
                    ]
                }
            }
        }
        // A simple JSON serializer installed for each region
        def jsonMapSerializer = new DbRegionSerializer() {
            KeyValue serialize(KeyValue keyValue) {
                new KeyValue(keyValue.key().bytes, mapper.writeValueAsBytes(keyValue.value()))
            }

            KeyValue deserialize(KeyValue keyValue) {
                new KeyValue(new String(keyValue.key()), mapper.readValue(keyValue.value(), Map))
            }
        }
        store.addRegion('people', jsonMapSerializer)
        store.addRegion('dogs', jsonMapSerializer)

        when:
        def key = "xxxxx".bytes
        def value = '{"person": {"name": "bob"}, "dog": {"name": "fido"}}'.bytes
        store.ingest(key, value)

        then:
        store.getParsed('people', 'aaa'.bytes).value() == [name: 'bob']
        store.getParsed('dogs', 'bbb'.bytes).value() == [name: 'fido']
        appPersonStorage.get('aaa') == [name: 'bob']
        appDogStorage.get('bbb') == [name: 'fido']

        when:
        // As in the previous 1:1 mapping test, this one assumes the user is able to supply the storage keys from the
        // message key. Note the arbitrary storage keys produced by the MessageSplitter and used for storage.
        store.ingest(key, null)

        then:
        !store.containsKey('people', 'aaa'.bytes)
        !store.containsKey('dogs', 'bbb'.bytes)
        !appPersonStorage.containsKey('aaa')
        !appDogStorage.containsKey('bbb')
    }

    def 'it lets users configure a region that stores key mappings'() {
        // This isn't really different than typical usage. It's just an example for users that want to keep track of
        // key mappings with the internal database instead of tracking it separately on their own. In other words, it
        // lets users to stateful key mapping on removes, so that even without the context of a value, an input key can
        // still be mapped to multiple output keys when the latter can't be directly derived from the former an without
        // the user needing to keep track of such mapping separately.
        given:
        def appOneStorage = [:]
        def appTwoStorage = [:]
        def appRedStorage = [:]
        def appBlueStorage = [:]
        def topology = new AppMemoryTopology([
                one: appOneStorage,
                two: appTwoStorage,
                red: appRedStorage,
                blue: appBlueStorage,
        ])
        def store = new GlobalStateStore(topology)
        store.messageParser = new AbstractNullValuePassingMessageParser() {
            @Override
            Object parseKey(Object key) {
                new String(key)
            }

            @Override
            Object parseValue(Object value) {
                new String(value)
            }
        }
        store.messageSplitter = new MessageSplitter() {
            @Override
            List<StorageOperation> split(KeyValue keyValue) {
                // this is awkward; it doesn't feel like it's the splitter's decision about how to handle puts vs
                // removes. How do I go from parser to splitter during a remove?
                // Maybe a message splitter only activates on non-null values?
                // Of course, then I have to handle mapping keys when values are null.
                // More and more, it seems like my interfaces need to handle key and value mapping separately, with the
                // opposite thing in context. I.E. when mapping a key, the value is available to the user, and when
                // mapping the value, the key is available to the user.
                // Doing it this way (separate key and value mappings) will allow for more abstract implementations and
                // ... what was i think about here?
                // Overall, i'm thinking there might be a better way for splitting to emerge into keys and values that
                // doesn't rely so completely on every MessageSplitter implementation from having to do an
                // 'if value == null then ... else ... fi' because as it stands, I think every single one needs that.
                // How to separate create/update from delete better?
                //
                // do i need to version these things for simpler upgrading the interface?
                // .
                return [new Put]
            }
        }

    }

    // how does that ^^ work? a user could split a message like key-123 -> {person: {name: bob}, dog: {name: fido}} into
    // key-bob -> {name: bob} and key-fido -> {name: fido}
    // Then, it's impossible to know the storage keys without separately indexing that key-123 relates to key-bob and
    // key-fido. But, is that always guaranteed? Suppose the next instance of key-123 only contains bob?
    // I guess that should map to a removal of fido, which would correspondingly have to update the key index to remove
    // the key-123 -> key-fido mapping. This would rely on users' correctly implementing their message splitters.
    // Can I keep track of the keys emitted by message splits and manage the indexing automatically then? I think yes.
    // As considered below, a user could request automatic key-mapping, and the system would just do it. Not on by default
    // due to the extra overhead.

    // also:
    // update and remove operations informed by existing storage state
    // - does letting removes access storage state solve the key mapping problem?
    // - suppose the key is modified using existing state on the way in; then the resulting key can't be known only from the raw key
    // -- add support for a KeyMapper - maps message keys to resulting storage keys? triggering this somehow makes the store maintin the index automatically

}
