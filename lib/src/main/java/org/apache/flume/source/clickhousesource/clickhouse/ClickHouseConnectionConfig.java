package org.apache.flume.source.clickhousesource.clickhouse;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Objects;

public class ClickHouseConnectionConfig implements Serializable {

    private final Boolean ssl;
    private final String host;
    private final Integer port;
    private final String user;
    private final String password;

    public ClickHouseConnectionConfig(
            String host,
            Integer port,
            String user,
            String password,
            Boolean ssl
    ) {
        this.ssl = ssl;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public Boolean isSsl() {
        return ssl;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public  boolean isConfigured() {
        return ssl != null && !StringUtils.isEmpty(host) && port != null
                && !StringUtils.isEmpty(user) && !StringUtils.isEmpty(password);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClickHouseConnectionConfig that = (ClickHouseConnectionConfig) o;
        return ssl == that.ssl &&
                Objects.equals(host, that.host) &&
                Objects.equals(port, that.port) &&
                Objects.equals(user, that.user) &&
                Objects.equals(password, that.password);
    }
}