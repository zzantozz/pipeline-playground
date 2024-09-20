package org.example.globalstate.services;

public record Dog(String name, int legs) {
    @Override
    public String toString() {
        return "my own special dog named " + name;
    }
}
