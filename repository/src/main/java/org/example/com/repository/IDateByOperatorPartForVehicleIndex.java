package org.example.com.repository;

import org.example.com.repository.data.PartedKeyValues;

public interface IDateByOperatorPartForVehicleIndex {

  PartedKeyValues getVehiclesOnDateOperatorPart(long from, long to, final String operator);

  void writeVehicleOnDatePartOperator(long timestamp, String operator, final String vehicleId);
  void invalidateCache(long timestamp, final String operator);

  void deleteAllData();
  void invalidateAllCache();

}
