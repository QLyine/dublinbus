package org.example.com.repository;

import org.example.com.repository.data.PartedKeyValues;

public interface IDatePartitionForOperatorIndex {

  PartedKeyValues getOperatorsOnDatePart(long from, long to);

  void writeOperatorsOnDatePart(long timestamp, String operator);

  void invalidateCache(long timestamp);

  void deleteAllData();

  void invalidateAllCache();
}
