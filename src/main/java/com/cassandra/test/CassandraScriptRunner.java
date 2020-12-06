package com.cassandra.test;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.cassandra.test.CassandraConnection.createSession;

public class CassandraScriptRunner {

    private static final Logger log = LoggerFactory.getLogger(CassandraScriptRunner.class);

    public static void initDB(
            String clusterName,
            List<String> contactPoints,
            int port,
            String userName,
            String password,
            boolean useSSL,
            String consistencyLevel,
            String keySpace,
            String dcName
    ) {
        CqlSession session = createSession(clusterName, contactPoints, port, null, userName, password, useSSL, consistencyLevel, dcName).getCqlSession();
        runScript(session, "/init.cql", keySpace);
    }

    public static void runScript(CqlSession session, String scriptName, String keySpace) {
        try {
            // https://lankydanblog.com/2018/04/15/interacting-with-cassandra-using-the-datastax-java-driver/
            String[] statements = StringUtils.split(IOUtils.toString(CassandraScriptRunner.class.getResourceAsStream(scriptName), "UTF-8"), ";");
            ArrayList<String> statementsFinal = new ArrayList<>();
            for (String statement: statements){
                statementsFinal.add(statement.replaceAll("DB-NAME", keySpace));
            }
            statementsFinal.stream().map(statement -> statement + ";").forEach(session::execute);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
