package org.apache.flume.source.clickhousesource.clickhouse;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionDecorator implements AutoCloseable {

    private final Connection connection;
    private final AtomicInteger usedConnectionsCounter;

    public ConnectionDecorator(@NonNull Connection connection, @NonNull AtomicInteger usedConnectionsCounter) {
        this.connection = connection;
        this.usedConnectionsCounter = usedConnectionsCounter;
    }

    public @NonNull Connection getConnection() {
        return connection;
    }

    @Override
    public void close() throws SQLException {
        usedConnectionsCounter.decrementAndGet();
        connection.close();
    }
}
  