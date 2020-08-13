package org.example.com.repository;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vavr.control.Try;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.example.com.data.utils.VehicleData;
import org.example.com.repository.data.PartedKeyValues;

@SuppressWarnings("Convert2MethodRef")
public class VehiclesRepositoryImpl implements IVehiclesRepository {

  private final IDatePartitionForVehicleIndex vehicleIndex;
  private final IDatePartitionForOperatorIndex operatorIndex;
  private final IDateByOperatorPartForVehicleIndex operatorVehicleIndex;

  private final AerospikeClient client;

  private static final String NAMESPACE = "test";
  private static final String SETNAME = "vehicles";
  private static final String DATA_BIN = "databin";

  private final Cache<String, TreeSet<VehicleData>> cache;
  private final ObjectMapper objectMapper;
  private static final TypeReference<TreeSet<VehicleData>> TYPE_REFERENCE = new TypeReference<TreeSet<VehicleData>>() {
  };


  public VehiclesRepositoryImpl(final String host, final int port) {
    this(new AerospikeClient(host, port));
  }

  public VehiclesRepositoryImpl(final AerospikeClient client) {
    this.client = client;
    this.cache = Caffeine.newBuilder()
        .maximumSize(1_000)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .build();
    this.vehicleIndex = new DatePartitionForVehicleIndexImpl(client);
    this.operatorVehicleIndex = new DateByOperatorPartForVehicleImpl(client);
    this.operatorIndex = new DatePartitionForOperatorIndexImpl(client);
    this.objectMapper = new ObjectMapper();
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
    this.objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    this.objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    this.objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
  }

  public Set<String> getOperators(final long from, final long to) {
    final PartedKeyValues operatorsOnDatePart = operatorIndex.getOperatorsOnDatePart(from, to);
    final IntList hours = operatorsOnDatePart.getHours();
    if (hours.isEmpty()) {
      return Collections.emptySet();
    }
    final long startIndexTsMicro = TimeUnit.HOURS.toMicros(hours.getInt(0));
    final long endIndexTsMicro = TimeUnit.HOURS.toMicros(hours.getInt(hours.size() - 1));
    final long toEndOfBucket =
        TimeUnit.HOURS.toMicros(operatorsOnDatePart.getHours().getInt(0)) + TimeUnit.HOURS.toMicros(
            1) - 1L;
    if (from <= startIndexTsMicro && to >= toEndOfBucket) {
      return operatorsOnDatePart.getTimeInHoursByKeyValue().keySet();
    }
    // otherwise we have to check the edge case
    if (hours.size() == 1) {
      return operatorsOnDatePart.getValuesOnEachHour()
          .get(0)
          .stream()
          .flatMap(operatorId -> getVehiclesOfOperator(from, to, operatorId).stream())
          .filter(e -> e.getTs() >= from && e.getTs() <= to)
          .map(e -> e.getOperatorId())
          .collect(Collectors.toSet());
    }
    final Set<String> operators = new HashSet<>();
    int start = 0;
    int end = hours.size() - 1;
    if (from > startIndexTsMicro) {
      start++;
      final List<String> operatorsToFind = operatorsOnDatePart.getValuesOnEachHour().get(0);
      findIfOperatorsAreRunningAndAddToSet(from, toEndOfBucket, operatorsToFind, operators);
    }
    if (to < toEndOfBucket) {
      end--;
      final List<String> operatorsToFind = operatorsOnDatePart.getValuesOnEachHour()
          .get(hours.size() - 1);
      findIfOperatorsAreRunningAndAddToSet(endIndexTsMicro, to, operatorsToFind, operators);
    }

    if (start >= end) {
      return operators;
    }
    for (int i = start; i < end; i++) {
      operators.addAll(operatorsOnDatePart.getValuesOnEachHour().get(i));
    }
    return operators;
  }

  public NavigableSet<VehicleData> getVehiclesOfOperator(
      final long from, final long to, final String operator
  ) {
    final PartedKeyValues vehiclesOnDateOperatorPart = operatorVehicleIndex.getVehiclesOnDateOperatorPart(from,
        to,
        operator
    );
    final IntList hours = vehiclesOnDateOperatorPart.getHours();
    if (hours.isEmpty()) {
      return new TreeSet<>();
    }
    final List<String> ids = vehiclesOnDateOperatorPart.getTimeInHoursByKeyValue()
        .entrySet()
        .stream()
        .flatMap(e -> VehiclesRepositoryImpl.createKeys(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
    final TreeSet<VehicleData> vehicleData = readFromMultipleKeys(ids);
    VehiclesRepositoryImpl.trimTree(from, to, vehicleData);
    return vehicleData;
  }

  public NavigableSet<VehicleData> getVehiclesDataWithId(
      final long from, final long to, final String vehicleId
  ) {
    final PartedKeyValues vehiclesOnDatePart = vehicleIndex.getVehiclesOnDatePart(from, to);
    if (vehiclesOnDatePart.getTimeInHoursByKeyValue().containsKey(vehicleId)) {
      final IntList intList = vehiclesOnDatePart.getTimeInHoursByKeyValue().get(vehicleId);
      final List<String> ids = intList.stream()
          .map(e -> Utils.createTimedKey(e, vehicleId))
          .collect(Collectors.toList());
      final TreeSet<VehicleData> setOfVehicles = readFromMultipleKeys(ids);
      VehiclesRepositoryImpl.trimTree(from, to, setOfVehicles);
      return setOfVehicles;
    }
    return Collections.emptyNavigableSet();
  }

  /**
   * Writing Data should be done in map reduce fashion in order to invalidate caches. But for
   * purposes of this challenge that is ignored as I find that it gets out of the scope. Also there
   * is a problem of atomicity and that's one more reason it should be done in map reduce fashion.
   * This method is here to help to populate the data base only.
   */
  public void writeData(final VehicleData vehicleData) {
    final long ts = vehicleData.getTs();
    final long hours = TimeUnit.MICROSECONDS.toHours(ts);
    final String timedKey = Utils.createTimedKey(hours, vehicleData.getVehicleId());
    final Record record = client.get(null,
        new Key(VehiclesRepositoryImpl.NAMESPACE, VehiclesRepositoryImpl.SETNAME, timedKey)
    );
    final TreeSet<VehicleData> vehiclesOnBucket = mapFromRecord(record);
    vehiclesOnBucket.add(vehicleData);
    client.put(null,
        new Key(VehiclesRepositoryImpl.NAMESPACE, VehiclesRepositoryImpl.SETNAME, timedKey),
        mapToDataBin(vehiclesOnBucket)
    );

    vehicleIndex.writeVehicleOnDatePart(vehicleData.getTs(), vehicleData.getVehicleId());
    operatorIndex.writeOperatorsOnDatePart(vehicleData.getTs(), vehicleData.getOperatorId());
    operatorVehicleIndex.writeVehicleOnDatePartOperator(vehicleData.getTs(),
        vehicleData.getOperatorId(),
        vehicleData.getVehicleId()
    );

  }

  private static void trimTree(
      final long from, final long to, final TreeSet<VehicleData> vehicleData
  ) {
    while (!vehicleData.isEmpty() && from > vehicleData.first().getTs()) {
      vehicleData.pollFirst();
    }
    while (!vehicleData.isEmpty() && to < vehicleData.last().getTs()) {
      vehicleData.pollLast();
    }
  }

  private TreeSet<VehicleData> readFromMultipleKeys(final Collection<String> timedKeys) {
    final TreeSet<VehicleData> vehicleData = new TreeSet<>();
    Utils.doBatchReadWithCacheMapped(client,
        cache,
        VehiclesRepositoryImpl.NAMESPACE,
        VehiclesRepositoryImpl.SETNAME,
        timedKeys,
        batchRead -> mapFromRecord(batchRead.record)
    ).forEach((key, value) -> vehicleData.addAll(value));
    return vehicleData;
  }

  private Bin mapToDataBin(final TreeSet<VehicleData> data) {
    final String dataJson = Try.of(() -> objectMapper.writeValueAsString(data)).getOrElse("");
    return new Bin(VehiclesRepositoryImpl.DATA_BIN, dataJson);
  }

  private TreeSet<VehicleData> mapFromRecord(final Record record) {
    return Optional.ofNullable(record)
        .map(e -> e.getString(VehiclesRepositoryImpl.DATA_BIN))
        .flatMap(e -> Try.of(() -> (TreeSet<VehicleData>) objectMapper.readValue(e,
            VehiclesRepositoryImpl.TYPE_REFERENCE
        )).toJavaOptional())
        .orElseGet(() -> new TreeSet<>());
  }


  private void findIfOperatorsAreRunningAndAddToSet(
      final long from, final long to, final List<String> operators, final Set<String> toAdd
  ) {
    operators.forEach(e -> findIfOperatorIsRunningAndAddToSet(from, to, e, toAdd));
  }

  private void findIfOperatorIsRunningAndAddToSet(
      final long from, final long to, final String operator, final Set<String> toAdd
  ) {
    getVehiclesOfOperator(from, to, operator).stream()
        .filter(e -> e.getTs() >= from && e.getTs() <= to)
        .map(e -> e.getOperatorId())
        .forEach(e -> toAdd.add(e));
  }

  private static Stream<String> createKeys(final String key, final IntList timeinHours) {
    return timeinHours.stream().map(e -> Utils.createTimedKey(e, key));
  }

  @Override
  public void deleteAllData() {
    operatorVehicleIndex.deleteAllData();
    operatorIndex.deleteAllData();
    vehicleIndex.deleteAllData();
    Utils.deleteAll(client, VehiclesRepositoryImpl.NAMESPACE, VehiclesRepositoryImpl.SETNAME);
  }

  @Override
  public void invalidateAllCache() {
    operatorVehicleIndex.invalidateAllCache();
    operatorIndex.invalidateAllCache();
    vehicleIndex.invalidateAllCache();
    cache.asMap().clear();
  }
}
