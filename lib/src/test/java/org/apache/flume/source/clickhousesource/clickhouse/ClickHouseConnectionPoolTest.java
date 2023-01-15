package org.apache.flume.source.clickhousesource.clickhouse;

import lombok.Getter;
import lombok.Setter;
import org.assertj.core.api.Assertions;
import static org.assertj.db.api.Assertions.assertThat;
import org.assertj.db.type.Table;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Setter
@Getter
class ClickHouseConnectionPoolTest extends DefaultConnectionTest {
    private ClickHouseConnectionPool clickHouseConnectionPool;

    @Test
    @ExtendWith(CreateConnection.class)
    @DisplayName("test case: Подключение к базе данных корректное, создается пул соединений, генерации исключения не происходит.")
    void connectionToDB() {
        Assertions.assertThat(clickHouseConnectionPool)
                .isNotNull()
                .extracting(ClickHouseConnectionPool::getConnection)
                .extracting(ConnectionDecorator::getConnection)
                .isNotNull();
    }

    @Test
    @ExtendWith(CreateConnection.class)
    @DisplayName("test case: Не существует запрашиваемой базы данных. Генерируется исключение SQLException.")
    void connectionDataBaseNotExist() throws SQLException, IOException {
        ClickHouseDataSource clickHouseDataSource = generateClickHouseDataSource();
        try (Connection connection = clickHouseConnectionPool.getConnection().getConnection();
             Statement statement = connection.createStatement()) {
            ClickHouseProperties properties = clickHouseDataSource.getProperties();
            Assertions.assertThatCode(() -> statement.executeQuery(String.format("SELECT * FROM %s", properties.getDatabase().concat(".tableIsNotExist"))))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining(String.format("Code: 81. DB::Exception: Database %s doesn't exist. (UNKNOWN_DATABASE) (version 21.12.3.32 (official build))", properties.getDatabase()));
        }
    }

    @Test
    @ExtendWith({CreateConnection.class, DropDataBase.class})
    @DisplayName("test case: Не существует запрашиваемой таблицы. Генерируется исключение SQLException.")
    void connectionTableNotExist() throws SQLException, IOException {
        ClickHouseDataSource clickHouseDataSource = generateClickHouseDataSource();
        try (Connection connection = clickHouseConnectionPool.getConnection().getConnection();
             Statement statement = connection.createStatement()) {
            ClickHouseProperties properties = clickHouseDataSource.getProperties();
            Assertions.assertThatCode(() -> statement.executeQuery(String.format("CREATE DATABASE IF NOT EXISTS %s", properties.getDatabase()))).doesNotThrowAnyException();
            Table tableIsNotExist = new Table(clickHouseDataSource, "tableIsNotExist");
            assertThat(tableIsNotExist).doesNotExist();
            Assertions.assertThatCode(() -> statement.executeQuery(String.format("SELECT * FROM %s", properties.getDatabase().concat(".tableIsNotExist"))))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining(String.format("Code: 60. DB::Exception: Table %s doesn't exist. (UNKNOWN_TABLE) (version 21.12.3.32 (official build))", properties.getDatabase().concat(".tableIsNotExist")));
        }
    }

    @Test
    @ExtendWith({CreateConnection.class, DropDataBase.class})
    @DisplayName("test case: Запрашиваемая таблица и база данных существует. Генерирации исключения нет.")
    void connectionDataBaseAndTableExist() throws SQLException, IOException {
        ClickHouseDataSource clickHouseDataSource = generateClickHouseDataSource();
        try (Connection connection = clickHouseConnectionPool.getConnection().getConnection();
             Statement statement = connection.createStatement()) {
            ClickHouseProperties properties = clickHouseDataSource.getProperties();
            Assertions.assertThatCode(() -> statement.executeQuery(String.format("CREATE DATABASE IF NOT EXISTS %s", properties.getDatabase()))).doesNotThrowAnyException();
            Assertions.assertThatCode(() -> statement.executeQuery(String.format("CREATE TABLE IF NOT EXISTS %s (x String) ENGINE = Memory", properties.getDatabase().concat(".tableIsExist")))).doesNotThrowAnyException();
            Table tableIsExist = new Table(clickHouseDataSource, "tableIsExist");
            assertThat(tableIsExist)
                    .hasNumberOfColumns(1)
                    .column("x")
                    .isEmpty();
            Assertions.assertThatCode(() -> statement.executeQuery(String.format("SELECT * FROM %s", properties.getDatabase().concat(".tableIsExist")))).doesNotThrowAnyException();
        }
    }

    @Test
    @ExtendWith({CreateConnection.class, DropDataBase.class})
    @DisplayName("test case: Извлекаются существующие данные из таблицы.")
    void selectDataFromTable() throws SQLException, IOException {
        ClickHouseDataSource clickHouseDataSource = generateClickHouseDataSource();
        try (Connection connection = clickHouseConnectionPool.getConnection().getConnection();
             Statement statement = connection.createStatement()) {
            ClickHouseProperties properties = clickHouseDataSource.getProperties();
            Assertions.assertThatCode(() -> statement.executeQuery(String.format("CREATE DATABASE IF NOT EXISTS %s", properties.getDatabase()))).doesNotThrowAnyException();
            Assertions.assertThatCode(() -> statement.executeQuery(String.format("CREATE TABLE IF NOT EXISTS %s (x String) ENGINE = Memory", properties.getDatabase().concat(".tableIsExist")))).doesNotThrowAnyException();
            statement.executeQuery(String.format("INSERT INTO %s (x) VALUES ('x1'),('x2'),('x3')", properties.getDatabase().concat(".tableIsExist")));
            ResultSet resultSet = statement.executeQuery(String.format("SELECT * FROM %s", properties.getDatabase().concat(".tableIsExist")));
            Table tableIsExist = new Table(clickHouseDataSource, "tableIsExist");
            Collection<String> strings = new ArrayList<>();
            while (resultSet.next()) {
                strings.add(resultSet.getString("x"));
            }
            assertThat(tableIsExist)
                    .column("x")
                    .containsValues(strings.toArray(new String[0]));
        }
    }

    private static class CreateConnection implements BeforeTestExecutionCallback {
        private final int maxPoolSize = 1;
        private final int sqlQueryQueueSize = 2;

        @Override
        public void beforeTestExecution(ExtensionContext context) throws Exception {
            ClickHouseConnectionPoolTest clickHouseConnectionPoolTest = context.getTestInstance().map(ClickHouseConnectionPoolTest.class::cast).orElse(null);
            ClickHouseDataSource clickHouseDataSource = clickHouseConnectionPoolTest.generateClickHouseDataSource();
            clickHouseConnectionPoolTest.setClickHouseConnectionPool(new ClickHouseConnectionPool(
                    clickHouseDataSource.getProperties(),
                    maxPoolSize,
                    sqlQueryQueueSize));
        }
    }

    private static class DropDataBase implements AfterEachCallback {
        @Override
        public void afterEach(ExtensionContext context) throws Exception {
            ClickHouseConnectionPoolTest clickHouseConnectionPoolTest = context.getTestInstance().map(ClickHouseConnectionPoolTest.class::cast).orElse(null);
            ClickHouseConnectionPool clickHouseConnectionPool = clickHouseConnectionPoolTest.getClickHouseConnectionPool();
            try (Connection connection = clickHouseConnectionPool.getConnection().getConnection();
                 Statement statement = connection.createStatement()) {
                ClickHouseDataSource clickHouseDataSource = clickHouseConnectionPoolTest.generateClickHouseDataSource();
                ClickHouseProperties properties = clickHouseDataSource.getProperties();
                Assertions.assertThatCode(() -> statement.executeQuery(String.format("DROP DATABASE IF EXISTS %s", properties.getDatabase()))).doesNotThrowAnyException();
            }
        }
    }
}