package org.example.globalstate.services;

import org.example.globalstate.core.KeyValue;
import org.example.globalstate.core.Marshaller;
import org.example.globalstate.core.Person;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersonMarshaller implements Marshaller {
    public static final Pattern pattern = Pattern.compile("name=(.*),age=(.*)");

    @Override
    public KeyValue marshal(Map m) {
        String name = (String) m.get("name");
        return new KeyValue(name, "name=" + name + ",age=" + m.get("age"));
    }

    @Override
    public Object unmarshal(KeyValue kv) {
        Matcher matcher = pattern.matcher(kv.value().toString());
        matcher.find();
        String name = matcher.group(1);
        int age = Integer.parseInt(matcher.group(2));
        return new Person(name, age);
    }
}
