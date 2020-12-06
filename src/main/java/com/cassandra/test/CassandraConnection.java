package com.cassandra.test;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraConnection {

    private static final Logger log = LoggerFactory.getLogger(CassandraConnection.class);

    private static void sleep(long sleepFor) {
        try {
            Thread.sleep(sleepFor);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static CassandraSession createSession(
            String clusterName,
            List<String> contactPoints,
            int port,
            String keyspace,
            String userName,
            String password,
            boolean useSSL,
            String consistencyLevel,
            String dcName
    ) {
        CqlSessionBuilder clusterBuilder = CqlSession.builder();

        if (useSSL) {
            DriverConfigLoader loader = DriverConfigLoader.fromClasspath("ssl.conf");
            clusterBuilder.withConfigLoader(loader);
        }

        if (StringUtils.isNotEmpty(keyspace)) {
            clusterBuilder.withKeyspace(keyspace);
        }

        String dcNameWithFallback = StringUtils.isEmpty(dcName) ? "datacenter1" : dcName;

        ConsistencyLevel level = DefaultConsistencyLevel.valueOf(consistencyLevel);
        List<InetSocketAddress> contactPointsSocks = contactPoints.stream().map(x -> new InetSocketAddress(x, port)).collect(Collectors.toList());
        CqlSession session = clusterBuilder.withApplicationName(clusterName)
                .addContactPoints(contactPointsSocks)
                .withLocalDatacenter(dcNameWithFallback)
                .withAuthCredentials(userName, password).build();

        return new CassandraSession(session, level);

    }
}
