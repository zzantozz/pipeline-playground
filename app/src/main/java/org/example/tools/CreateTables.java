package org.example.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CreateTables {
    public static void main(String args[]) throws Exception {
        Class.forName("org.postgresql.Driver");
        final Connection c = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/postgres",
                "postgres", "admin");
        Statement statement = c.createStatement();
        statement.executeUpdate("create table test(id int primary key, name varchar, address text)");
        statement.close();
        c.close();
    }
}
