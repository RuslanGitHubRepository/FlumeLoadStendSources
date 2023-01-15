package org.apache.flume.source.clickhousesource.clickhouse;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum ReQueryErrorCode {
    CANNOT_PARSE_TEXT(6),
    CANNOT_READ_ALL_DATA_FROM_TAB_SEPARATED_INPUT(21),
    CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT(22),
    CANNOT_READ_FROM_ISTREAM(23),
    CANNOT_WRITE_TO_OSTREAM(24),
    CANNOT_PARSE_ESCAPE_SEQUENCE(25),
    CANNOT_READ_ALL_DATA(33),
    CHECKSUM_DOESNT_MATCH(40),
    RECEIVED_ERROR_FROM_REMOTE_IO_SERVER(86),
    UNKNOWN_PACKET_FROM_CLIENT(99),
    UNKNOWN_PACKET_FROM_SERVER(100),
    UNEXPECTED_PACKET_FROM_CLIENT(101),
    UNEXPECTED_PACKET_FROM_SERVER(102),
    CANNOT_BLOCK_SIGNAL(109),
    CANNOT_UNBLOCK_SIGNAL(110),
    CANNOT_MANIPULATE_SIGSET(111),
    CANNOT_WAIT_FOR_SIGNAL(112),
    THERE_IS_NO_SESSION(113),
    CANNOT_CLOCK_GETTIME(114),
    UNKNOWN_SETTING(115),
    NOT_FOUND_NODE(142),
    FOUND_MORE_THAN_ONE_NODE(143),
    TOO_SLOW(160),
    IP_ADDRESS_NOT_ALLOWED(195),
    DNS_ERROR(198),
    NO_FREE_CONNECTION(203),
    CANNOT_FSYNC(204),
    SOCKET_TIMEOUT(209),
    NETWORK_ERROR(210),
    LIMIT_EXCEEDED(290),
    RECEIVED_EMPTY_DATA(295),
    SYSTEM_ERROR(425),
    UNKNOWN_EXCEPTION(1002);

    private static final Map<Integer, ReQueryErrorCode> byCodes;

    static {
        byCodes = Arrays.stream(values())
                .collect(Collectors.toMap(ReQueryErrorCode::getCode, Function.identity()));
    }

    public final Integer code;

    public Integer getCode() {
        return code;
    }

    ReQueryErrorCode(Integer code) {
        this.code = code;
    }

    public static boolean isCode(int code) {
        return byCodes.containsKey(code);
    }

    public static ReQueryErrorCode fromCode(Integer code) {
        return byCodes.get(code);
    }
}