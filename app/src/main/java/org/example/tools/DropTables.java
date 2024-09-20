package org.example.tools;

import org.example.core.Settings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DropTables {
    public static void main(String args[]) throws Exception {
        Class.forName("org.postgresql.Driver");
        final Connection c = DriverManager.getConnection(
                "jdbc:postgresql://" + Settings.POSTGRES_HOST + ":5432/postgres",
                "postgres", "admin");
        Statement statement = c.createStatement();
        statement.executeUpdate("drop table drop_rule");
        statement.executeUpdate("drop table query");
        statement.close();
        c.close();
    }
}
