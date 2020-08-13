package org.example.com.repository;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.example.com.data.utils.DU;
import org.example.com.repository.data.PartedKeyValues;

@SuppressWarnings("ALL")
public class DatePartitionForVehicleIndexImpl implements IDatePartitionForVehicleIndex {

  private final AerospikeClient client;
  final static String NAMESPACE = "test";
  final static String SETNAME = "datevehiclepart";
  private final static String BIN_NAME = "data";


  private final Cache<String, BatchRead> cache;

  public DatePartitionForVehicleIndexImpl(final String host, final int port) {
    this(new AerospikeClient(host, port));
  }

  public DatePartitionForVehicleIndexImpl(AerospikeClient client) {
    this.client = client;
    this.cache = Caffeine.newBuilder()
        .maximumSize(100_000)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .build();
  }

  @Override
  public PartedKeyValues getVehiclesOnDatePart(final long from, final long to) {
    final List<String> collect = Utils.getIntervalsInHours(from, to)
        .mapToObj(e -> e + "")
        .collect(Collectors.toList());
    final TreeMap<Integer, List<String>> integers = new TreeMap<>();
    final Map<String, IntList> timesByOperator = new HashMap<>();

    Utils.doBatchReadWithCache(this.client, cache, NAMESPACE, SETNAME, collect, batchRead -> {
      final Map<?, ?> map = Optional.ofNullable(batchRead.record)
          .map(e -> e.getMap(BIN_NAME))
          .orElse(Collections.emptyMap());
      if (!DU.isMapEmptyOrNull(map)) {
        map.keySet()
            .stream()
            .map(e -> Utils.parseTimedKey(e.toString()))
            .filter(e -> e.isSuccess())
            .map(e -> e.get())
            .filter(e -> Utils.isContained(from, to, e._1))
            .forEach(e -> {
              DU.appendToCollection(integers, e._1, e._2, () -> new ArrayList<>());
              DU.appendToCollection(timesByOperator, e._2, e._1, () -> new IntArrayList());
            });
      }
    });

    final Iterator<Entry<Integer, List<String>>> iterator = integers.entrySet().iterator();
    final IntList intList = new IntArrayList(integers.size());
    final List<List<String>> values = new ArrayList<List<String>>(integers.size());
    while (iterator.hasNext()) {
      Entry<Integer, List<String>> next = iterator.next();
      List<String> value = next.getValue();
      Integer key = next.getKey();
      intList.add(key);
      values.add(value);
    }
    return new PartedKeyValues(intList, values, timesByOperator);
  }

  @Override
  public void writeVehicleOnDatePart(final long timestamp, final String vehicle) {
    final long hours = TimeUnit.MICROSECONDS.toHours(timestamp);
    final String timedKey = Utils.createTimedKey(hours, vehicle);
    final Operation put = MapOperation.put(MapPolicy.Default,
        BIN_NAME,
        Value.get(timedKey),
        Value.get(0)
    );
    client.operate(null, new Key(NAMESPACE, SETNAME, hours + ""), put);
  }

  @Override
  public void invalidateCache(final long timestamp) {
    final long hours = TimeUnit.MICROSECONDS.toHours(timestamp);
    cache.asMap().remove(hours + "");
  }

  @Override
  public void deleteAllData() {
    Utils.deleteAll(client, NAMESPACE, SETNAME);
  }

  @Override
  public void invalidateAllCache() {
    cache.asMap().clear();
  }

}
