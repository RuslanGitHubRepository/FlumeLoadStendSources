package org.apache.flume.source.clickhousesource.clickhouse;

public interface ReQueryParam {
    default long getTime(){
        return 1000;
    }
    default int getRepeatCount(){
        return 5;
    }
    default void repeat(){}
}

