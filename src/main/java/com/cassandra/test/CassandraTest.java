package com.cassandra.test;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CassandraTest {

    public static void main(String args[]) throws Exception {

        String clusterName = "Test Cluster";
        ArrayList<String> contactPoints = new ArrayList<String>() {{
            add("127.0.0.1");
        }};
        int port = 9042;
        String testuser = "testuser";
        String password = "fDJj8yMvbDwzaKZzpBoIEPz7b";
        boolean useSSL = false;
        String consistencyLevel = "ONE";
        String keySpaceName = "test_keyspace";
        String dbName = "datacenter1";

        CassandraScriptRunner.initDB(
                clusterName,
                contactPoints,
                port,
                testuser,
                password,
                useSSL,
                consistencyLevel,
                keySpaceName,
                dbName
                );


        CassandraSession session = CassandraConnection.createSession(
                clusterName,
                contactPoints,
                port,
                keySpaceName,
                testuser,
                password,
                useSSL,
                consistencyLevel,
                dbName
        );

        String fooValue = String.valueOf(System.currentTimeMillis());
        String barValue = String.valueOf(System.currentTimeMillis());

        Map map = new HashMap<>();
        map.put("foo", fooValue);
        map.put("bar", barValue);
        String query = "INSERT INTO test_connection (foo, bar) VALUES (:foo, :bar)";
        SimpleStatement statement = SimpleStatement.newInstance(query, map).setConsistencyLevel(session.getConsistencyLevel());
        session.getCqlSession().execute(statement);


        String barValueResult = null;
        query = "SELECT bar FROM test_connection WHERE foo = :foo";
        statement = SimpleStatement.newInstance(query, map).setConsistencyLevel(session.getConsistencyLevel());
        ResultSet rs = session.getCqlSession().execute(statement);
        Row row = rs.all().get(0);
        barValueResult = row.getString("bar");

        if (barValue.equals(barValueResult)) {
            System.out.println("works as expected");
            System.exit(0);
        } else {
            System.out.println("NOT WORKING");
            System.exit(1);
        }
    }
}