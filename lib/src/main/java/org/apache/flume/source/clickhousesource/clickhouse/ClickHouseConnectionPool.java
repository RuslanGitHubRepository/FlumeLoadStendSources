package org.apache.flume.source.clickhousesource.clickhouse;

import com.clickhouse.jdbc.ClickHouseDriver;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.avro.reflect.Nullable;
import org.apache.flume.FlumeException;
import org.checkerframework.checker.nullness.qual.NonNull;
import ru.yandex.clickhouse.settings.ClickHouseConnectionSettings;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class ClickHouseConnectionPool {
    private final Path certificate;
    private final ComboPooledDataSource connectionPool;
    private final int sqlQueryQueueSize;
    private final AtomicInteger usedConnectionsCounter;

    public ClickHouseConnectionPool(@NonNull ClickHouseProperties clickHouseProperties,
                                    @NonNull int maxPoolSize,
                                    @NonNull int sqlQueryQueueSize) {
        this.certificate = Objects.isNull(clickHouseProperties.getSslRootCertificate()) ? null : Paths.get(clickHouseProperties.getSslRootCertificate());
        this.sqlQueryQueueSize = sqlQueryQueueSize;
        this.usedConnectionsCounter = new AtomicInteger();
        connectionPool = new ComboPooledDataSource();
        try {
            connectionPool.setDriverClass(ClickHouseDriver.class.getName());
        } catch (Exception e) {
            throw new RuntimeException("Connection pool unexpected error", e);
        }
        connectionPool.setJdbcUrl(String.format("jdbc:clickhouse://%s:%s/", clickHouseProperties.getHost(), clickHouseProperties.getPort()));
        connectionPool.setInitialPoolSize(Math.min(10, maxPoolSize));
        connectionPool.setMinPoolSize(Math.min(10, maxPoolSize));
        connectionPool.setAcquireIncrement(Math.min(5, maxPoolSize));
        connectionPool.setMaxPoolSize(maxPoolSize);
        connectionPool.setCheckoutTimeout((int) Duration.ofSeconds(5).toMillis());
        connectionPool.setTestConnectionOnCheckin(false);
        connectionPool.setTestConnectionOnCheckout(false);
        connectionPool.setUser(clickHouseProperties.getUser());
        connectionPool.setPassword(clickHouseProperties.getPassword());
        connectionPool.setAcquireRetryAttempts(3);
        Properties properties = new Properties();
        properties.putAll(connectionPool.getProperties());
        properties.setProperty(ClickHouseConnectionSettings.SOCKET_TIMEOUT.getKey(), String.valueOf(clickHouseProperties.getSocketTimeout()));
        if (clickHouseProperties.getSsl()) {
            properties.setProperty(ClickHouseConnectionSettings.SSL.getKey(), "true");
            if (Objects.nonNull(certificate)) {
                properties.setProperty(ClickHouseConnectionSettings.SSL_ROOT_CERTIFICATE.getKey(), certificate.toString());
            }
        }
        connectionPool.setProperties(properties);
    }

    public void reset() {
        connectionPool.resetPoolManager();
    }

    public ConnectionDecorator getConnection() throws FlumeException {
        int usedConnectionsCount = usedConnectionsCounter.incrementAndGet();
        if (usedConnectionsCount > sqlQueryQueueSize) {
            usedConnectionsCounter.decrementAndGet();
            throw new FlumeException("clickhouse_overloaded");
        }
        try {
            return new ConnectionDecorator(connectionPool.getConnection(), usedConnectionsCounter);
        } catch (SQLException ex) {
            throw new FlumeException(ex);
        }
    }
}