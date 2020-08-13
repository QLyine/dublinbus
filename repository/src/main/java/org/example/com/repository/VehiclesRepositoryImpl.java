package org.example.com.repository;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import it.unimi.dsi.fastutil.Function;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.example.com.data.VehicleData;
import org.example.com.repository.data.PartedKeyValues;
import org.testng.util.Strings;

@SuppressWarnings("Convert2MethodRef")
public class VehiclesRepositoryImpl implements IVehiclesRepository {

  private final IDatePartitionForVehicleIndex vehicleIndex;
  private final IDatePartitionForOperatorIndex operatorIndex;
  private final IDateByOperatorPartForVehicleIndex operatorVehicleIndex;

  private final AerospikeClient client;

  private static final String NAMESPACE = "test";
  private static final String SETNAME = "vehicles";
  private static final String DATA_BIN = "databin";

  private final Cache<String, Optional<BucketHourVehicle>> cache;
  private final ObjectMapper objectMapper;
  private static final TypeReference<BucketHourVehicle> TYPE_REFERENCE = new TypeReference<BucketHourVehicle>() {
  };

  public VehiclesRepositoryImpl(final String host, final int port) {
    this(new AerospikeClient(host, port));
  }

  public VehiclesRepositoryImpl(final AerospikeClient client) {
    this.client = client;
    this.vehicleIndex = new DatePartitionForVehicleIndexImpl(client);
    this.operatorVehicleIndex = new DateByOperatorPartForVehicleImpl(client);
    this.operatorIndex = new DatePartitionForOperatorIndexImpl(client);
    this.objectMapper = new ObjectMapper();
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
    this.objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    this.objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    this.objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    this.cache = Caffeine.newBuilder()
        .maximumSize(1_000)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .build(new CacheLoader<String, Optional<BucketHourVehicle>>() {
          @Nullable
          @Override
          public Optional<BucketHourVehicle> load(@NonNull final String timedKey) throws Exception {
            final Record record = client.get(null,
                new Key(VehiclesRepositoryImpl.NAMESPACE, VehiclesRepositoryImpl.SETNAME, timedKey)
            );
            return mapFromRecord(record);
          }
        });
  }

  public Set<String> getOperators(final long from, final long to) {
    final PartedKeyValues operatorsOnDatePart = operatorIndex.getOperatorsOnDatePart(from, to);
    final IntList hours = operatorsOnDatePart.getHours();
    if (hours.isEmpty()) {
      return Collections.emptySet();
    }
    final long startIndexTsMicro = TimeUnit.HOURS.toMicros(hours.getInt(0));
    final long endIndexTsMicro = TimeUnit.HOURS.toMicros(hours.getInt(hours.size() - 1));
    final long toEndOfBucket = endIndexTsMicro + TimeUnit.HOURS.toMicros(1) - 1L;
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
    int end = hours.size();
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

    final TreeSet<VehicleData> vehicleData = new TreeSet<>();
    readFromMultipleKeys(ids).forEach((key, maybeVehicleData) -> maybeVehicleData.ifPresent(
        bucketHourVehicleData -> vehicleData.addAll(bucketHourVehicleData.vehicleData)));

    VehiclesRepositoryImpl.trimTree(from, to, vehicleData);
    return vehicleData;
  }


  @Override
  public Set<String> getVehiclesStoppedOfOperator(
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

    final Map<String, Boolean> result = new HashMap<>();
    readFromMultipleKeys(ids).entrySet()
        .parallelStream()
        .map(e -> isStoppedOnTimeFrame(from, to, e.getKey(), e.getValue()))
        .forEach(e -> {
          final String vehicleId = e._1;
          final boolean stoppedOnTimeFrame = e._2;
          result.computeIfAbsent(vehicleId, s -> stoppedOnTimeFrame);
          result.computeIfPresent(vehicleId, (k, v) -> v & stoppedOnTimeFrame);
        });

    return result.entrySet()
        .stream()
        .filter(e -> e.getValue())
        .map(e -> e.getKey())
        .collect(Collectors.toSet());

  }

  private static final Tuple2 EMPTY = new Tuple2<>("", false);

  private static final Tuple2<String, Boolean> isStoppedOnTimeFrame(
      final long from,
      final long to,
      final String timedKey,
      final Optional<BucketHourVehicle> bucketHourVehicle
  ) {
    final Optional<Tuple2<Integer, String>> timedKeyParsed = Utils.parseTimedKey(timedKey)
        .toJavaOptional();
    if (timedKeyParsed.isPresent() && bucketHourVehicle.isPresent()) {
      final String vehicleId = timedKeyParsed.get()._2;
      final BucketHourVehicle bucket = bucketHourVehicle.get();
      int stopped = bucket.stopped;
      final TreeSet<VehicleData> vehicleData = new TreeSet<>(bucket.vehicleData);
      while (!vehicleData.isEmpty() && from > vehicleData.first().getTs()) {
        if (isVehicleStopped(vehicleData.pollFirst())) {
          stopped--;
        }
      }
      while (!vehicleData.isEmpty() && to < vehicleData.last().getTs()) {
        if (isVehicleStopped(vehicleData.pollLast())) {
          stopped--;
        }
      }
      return new Tuple2<>(vehicleId, vehicleData.size() > 0 && stopped == vehicleData.size());
    }
    return EMPTY;
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
      final TreeSet<VehicleData> vehicleData = new TreeSet<>();
      readFromMultipleKeys(ids).forEach((key, maybeVehicleData) -> {
        maybeVehicleData.ifPresent(bucketHourVehicleData -> vehicleData.addAll(bucketHourVehicleData.vehicleData));
      });

      VehiclesRepositoryImpl.trimTree(from, to, vehicleData);
      return vehicleData;
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

    final BucketHourVehicle vehiclesOnBucket = cache.get(timedKey,
        (Function<String, Optional<BucketHourVehicle>>) key -> Optional.empty()
    )
        .orElseGet(() -> new BucketHourVehicle(vehicleData.getVehicleId(), new TreeSet<>(), 0));

    vehiclesOnBucket.vehicleData.add(vehicleData);
    if (isVehicleStopped(vehicleData)) {
      vehiclesOnBucket.stopped++;
    }

    client.put(WritePolicies.getSendKeyPolicy(),
        new Key(VehiclesRepositoryImpl.NAMESPACE, VehiclesRepositoryImpl.SETNAME, timedKey),
        mapToDataBin(vehiclesOnBucket)
    );
    cache.put(timedKey, Optional.of(vehiclesOnBucket));

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

  private Map<String, Optional<BucketHourVehicle>> readFromMultipleKeys(final Collection<String> timedKeys) {
    final Map<String, Optional<BucketHourVehicle>> timedKeysBuckets = new HashMap<>();
    Utils.doBatchReadWithCacheMapped(client,
        cache,
        VehiclesRepositoryImpl.NAMESPACE,
        VehiclesRepositoryImpl.SETNAME,
        timedKeys,
        batchRead -> mapFromRecord(batchRead.record)
    ).forEach((key, value) -> timedKeysBuckets.put(key, value));
    return timedKeysBuckets;
  }

  private Bin mapToDataBin(final BucketHourVehicle data) {
    final String dataJson = Try.of(() -> objectMapper.writeValueAsString(data)).getOrElse("");
    return new Bin(VehiclesRepositoryImpl.DATA_BIN, dataJson);
  }

  private Optional<BucketHourVehicle> mapFromRecord(final Record record) {
    return Optional.ofNullable(record)
        .map(e -> e.getString(VehiclesRepositoryImpl.DATA_BIN))
        .flatMap(e -> Try.of(() -> (BucketHourVehicle) objectMapper.readValue(e,
            VehiclesRepositoryImpl.TYPE_REFERENCE
        ))
            .onFailure(t -> t.printStackTrace())
            .toJavaOptional());
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

  public static boolean isVehicleStopped(final VehicleData vehicleData) {
    return Objects.nonNull(vehicleData) && !Strings.isNullOrEmpty(vehicleData.getStopId())
        && vehicleData.isStopped();
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static final class BucketHourVehicle {

    private String id;
    private NavigableSet<VehicleData> vehicleData;
    private int stopped;


  }
}
