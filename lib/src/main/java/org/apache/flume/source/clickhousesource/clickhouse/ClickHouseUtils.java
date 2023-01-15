package org.apache.flume.source.clickhousesource.clickhouse;

import org.apache.flume.FlumeException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseUtils {
    private final static Logger logger = LoggerFactory.getLogger(ClickHouseUtils.class);
    public static  <T> T executeWithRepeat(ClickHouseConnection clickHouseConnection,
                                    @NonNull String query,
                                    ResultHandler<T> handler,
                                    HashMap<ClickHouseQueryParam, String> additionalDBParams,
                                    Map<String, String> additionalRequestParams,
                                    ReQueryParam reQueryParam) throws FlumeException {
        Pattern pattern = Pattern.compile("(?i)^SELECT*");
        int repeatCount = 0;
        T result = null;
        while (repeatCount <= reQueryParam.getRepeatCount()) {
            try {
                try (ResultSet resultSet = clickHouseConnection.createStatement().executeQuery(
                        query,
                        additionalDBParams,
                        null,
                        additionalRequestParams)) {
                    if (handler != null) result = handler.handle(resultSet);
                }
                break;
            } catch (SQLException e) {
                repeatCount++;
                if (!pattern.matcher(query.trim()).find()) {
                    throw new FlumeException(e);
                }
                Matcher matcher = Pattern.compile("(?i)(code:)\\s([^,.]*)").matcher(e.getMessage());
                int codeError = -1;
                if (matcher.find()) {
                    codeError = Integer.parseInt(matcher.group(2));
                }
                if (ReQueryErrorCode.isCode(codeError) && repeatCount <= reQueryParam.getRepeatCount()) {
                    reQueryParam.repeat();
                    logger.debug("query: " + query + ", execute error: " + codeError +" "+ ReQueryErrorCode.fromCode(codeError) + ", attempt " + repeatCount);
                    try {
                        Thread.sleep(reQueryParam.getTime());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    throw new FlumeException(e);
                }
            }
        }
        return result;
    }
}
