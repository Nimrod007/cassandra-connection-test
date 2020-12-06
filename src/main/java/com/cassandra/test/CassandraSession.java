package com.cassandra.test;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

public class CassandraSession {

    private CqlSession cqlSession;
    private ConsistencyLevel consistencyLevel;

    public CassandraSession() {
    }

    public CassandraSession(CqlSession cqlSession, ConsistencyLevel consistencyLevel) {
        this.cqlSession = cqlSession;
        this.consistencyLevel = consistencyLevel;
    }

    public CqlSession getCqlSession() {
        return cqlSession;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setCqlSession(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }
}
