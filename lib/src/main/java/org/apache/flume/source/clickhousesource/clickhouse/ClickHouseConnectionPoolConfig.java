package org.apache.flume.source.clickhousesource.clickhouse;

import java.io.Serializable;
import java.rmi.server.RemoteObject;

public class ClickHouseConnectionPoolConfig implements Serializable {

    private final int maxPoolSize;
    private final int sqlQueryQueueSize;

    public ClickHouseConnectionPoolConfig(int maxPoolSize, int sqlQueryQueueSize) {
        this.maxPoolSize = maxPoolSize;
        this.sqlQueryQueueSize = sqlQueryQueueSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public int getSqlQueryQueueSize() {
        return sqlQueryQueueSize;
    }
}