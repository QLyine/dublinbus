package org.example.com.repository;

import java.util.NavigableSet;
import java.util.Set;
import org.example.com.data.VehicleData;

public interface IVehiclesRepository {

  Set<String> getOperators(long from, long to);

  NavigableSet<VehicleData> getVehiclesOfOperator(long from, long to, final String operator);

  Set<String> getVehiclesStoppedOfOperator(long from, long to, final String operator);

  NavigableSet<VehicleData> getVehiclesDataWithId(long from, long to, final String vehicleId);

  void writeData(VehicleData vehicleData);

  void deleteAllData();

  void invalidateAllCache();
}
