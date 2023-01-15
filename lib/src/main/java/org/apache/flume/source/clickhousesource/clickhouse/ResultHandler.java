package org.apache.flume.source.clickhousesource.clickhouse;

import org.apache.flume.FlumeException;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface ResultHandler<T> {

    T handle(ResultSet resultSet) throws SQLException;
}