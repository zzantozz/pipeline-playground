package org.example.globalstate.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.example.globalstate.core.InMemoryStorageBackend;
import org.example.globalstate.core.Marshaller;
import org.example.globalstate.core.RelationshipMarshaller;
import org.example.globalstate.core.StorageBackend;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GlobalStateServer {
    private StorageBackend storageBackend = new InMemoryStorageBackend();
    private Map<String, Marshaller> marshallersByType = new HashMap<>();

    public void start() throws Exception {
        marshallersByType.put("person", new PersonMarshaller());
        marshallersByType.put("dog", new DogMarshaller());
        marshallersByType.put("relationship", new RelationshipMarshaller());
        storageBackend.initialize();
        final ObjectMapper objectMapper = new ObjectMapper();

        var jetty = new Server(8080);
        jetty.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
                final String type;
                if (target.startsWith("/people")) {
                    type = "person";
                } else if (target.startsWith("/dogs")) {
                    type = "dog";
                } else if (target.startsWith("/relationships")) {
                    type = "relationship";
                } else {
                    throw new RuntimeException("Only /people and /dogs accepted");
                }
                String method = baseRequest.getMethod();
                final int status;
                final String message;
                if ("GET".equals(method)) {
                    final String id;
                    if (target.startsWith("/people/")) {
                        id = target.replace("/people/", "");
                    } else if (target.startsWith("/dogs/")) {
                        id = target.replace("/dogs/", "");
                    } else {
                        throw new RuntimeException("You need to get an id with /<thing>/<id>");
                    }
                    final var marshaller = marshallersByType.get(type);
                    message = marshaller.unmarshal(storageBackend.get(type, id)).toString();
                    status = 200;
                } else if ("POST".equals(method)) {
                    Map map = objectMapper.readValue(baseRequest.getInputStream(), Map.class);
                    Marshaller marshaller = marshallersByType.get(type);
                    storageBackend.put(type, marshaller.marshal(map));
                    status = 200;
                    message = "Object stored";
                } else {
                    status = 400;
                    message = "Check your input. Something is wrong.";
                }
                response.setStatus(status);
                response.getWriter().println(message);
                baseRequest.setHandled(true);
            }
        });
        jetty.start();
        jetty.join();
    }

    public static void main(String[] args) throws Exception {
        new GlobalStateServer().start();
    }
}
