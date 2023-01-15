package org.apache.flume.source.clickhousesource.clickhouse;

import org.junit.jupiter.api.TestInstance;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DefaultConnectionTest {
    protected DefaultConnectionTest() {}

    protected ClickHouseDataSource generateClickHouseDataSource() throws IOException {
        try (InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("config/analytic.db.yml")) {
            Properties properties = new Properties();
            properties.load(inputStream);
            ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
            clickHouseProperties.setUser(properties.getProperty("user"));
            clickHouseProperties.setPassword(properties.getProperty("password"));
            clickHouseProperties.setDatabase(properties.getProperty("name"));
            clickHouseProperties.setUseServerTimeZone(true);
            clickHouseProperties.setSsl(Boolean.parseBoolean(properties.get("ssl").toString()));
            clickHouseProperties.setSocketTimeout(Integer.parseInt(properties.getProperty("socketTimeoutSec")) * 1000);
            return new ClickHouseDataSource(
                    String.format("jdbc:clickhouse://%s:%d", properties.getProperty("host"), Integer.valueOf(properties.get("port").toString())),
                    clickHouseProperties
            );
        }
    }
}