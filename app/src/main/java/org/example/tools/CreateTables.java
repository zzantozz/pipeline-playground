package org.example.tools;

import org.example.core.Settings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CreateTables {
    public static void main(String args[]) throws Exception {
        Class.forName("org.postgresql.Driver");
        final Connection c = DriverManager.getConnection(
                "jdbc:postgresql://" + Settings.POSTGRES_HOST + ":5432/postgres",
                "postgres", "admin");
        Statement statement = c.createStatement();
        statement.executeUpdate("create table if not exists drop_rule(pk bigserial primary key, id text unique, value text)");
        statement.executeUpdate("create table if not exists query(pk bigserial primary key, id text unique, value text)");
        statement.close();
        c.close();
    }
}
