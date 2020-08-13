package org.example.com.repository;

import org.example.com.repository.data.PartedKeyValues;

public interface IDatePartitionForVehicleIndex {

  PartedKeyValues getVehiclesOnDatePart(long from, long to);

  void writeVehicleOnDatePart(long timestamp, String vehicle);

  void invalidateCache(long timestamp);

  void deleteAllData();

  void invalidateAllCache();
}
