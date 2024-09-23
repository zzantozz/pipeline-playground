package org.example.globalstate.core;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DogMarshaller implements Marshaller {
    public static final Pattern pattern = Pattern.compile("^name=(.*),legs=(.*)$");
    @Override
    public KeyValue marshal(Map m) {
        String name = (String) m.get("name");
        return new KeyValue(name, "name=" + name + ",legs=" + m.get("legs"));
    }

    @Override
    public Object unmarshal(KeyValue kv) {
        Matcher matcher = pattern.matcher(kv.value().toString());
        matcher.find();
        String name = matcher.group(1);
        int legs = Integer.parseInt(matcher.group(2));
        return new Dog(name, legs);
    }
}
