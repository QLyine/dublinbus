package org.example.com.repository;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.example.com.data.utils.DU;
import org.example.com.repository.data.AerospikeValues;

@SuppressWarnings("ALL")
public class Utils {


  /**
   * @param from timestamp in microseconds
   * @param to timestamp in microseconds
   * @return Stream of hours from [from] to [to]
   */
  public static IntStream getIntervalsInHours(
      final long from, final long to
  ) {
    final int fromHours = (int) TimeUnit.MICROSECONDS.toHours(from);
    final int toHours = (int) TimeUnit.MICROSECONDS.toHours(to);
    return IntStream.rangeClosed(fromHours, toHours);
  }


  /**
   * @param from timestamp in microseconds
   * @param to timestamp in microseconds
   * @return Stream of days from [from] to [to]
   */
  public static IntStream getIntervalsInDays(
      final long from, final long to
  ) {
    final int fromHours = (int) TimeUnit.MICROSECONDS.toDays(from);
    final int toHours = (int) TimeUnit.MICROSECONDS.toDays(to);
    return IntStream.rangeClosed(fromHours, toHours);
  }

  /**
   * @param from timestamp in microseconds
   * @param to timestamp in microseconds
   * @param keySpace keyspace
   * @return List of values in the following manner [hours:keyspace]
   */
  public static List<String> getKeySpaceIntervalsInHours(
      final long from, final long to, final String keySpace
  ) {
    return getIntervalsInHours(from, to).mapToObj(e -> createTimedKey(e, keySpace))
        .collect(Collectors.toList());
  }


  /**
   * @param from timestamp in microseconds
   * @param to timestamp in microseconds
   * @param keySpace keyspace
   * @return List of values in the following manner [days:keyspace]
   */
  public static List<String> getKeySpaceIntervalsInDays(
      final long from, final long to, final String keySpace
  ) {
    return getIntervalsInDays(from, to).mapToObj(e -> createTimedKey(e, keySpace))
        .collect(Collectors.toList());
  }

  public static String createOperatorVehicleKey(final String operator, final String vehicle) {
    return String.format("%s:%s", operator, vehicle);
  }

  public static String createTimedKey(final long time, final String key) {
    return String.format("%d:%s", time, key);
  }

  public static List<String> createTimedKey(final LongList timeInDays, final String key) {
    return timeInDays.stream()
        .map(e -> String.format("%d:%s", e, key))
        .collect(Collectors.toList());
  }

  public static Try<Tuple2<Integer, String>> parseTimedKey(final String s) {
    if (!Strings.isNullOrEmpty(s)) {
      String[] split = s.split(":");
      if (split.length > 1) {
        String tail = String.join(":", Arrays.asList(split).subList(1, split.length));
        return Try.of(() -> Integer.parseInt(split[0])).map(e -> new Tuple2<>(e, tail));
      }
    }
    return Try.failure(new Exception("invalid key"));
  }


  public static BatchRead createBatchRead(
      final String namespace,
      final String setname,
      final String id,
      final Supplier<List<String>> bins
  ) {
    final Key key = new Key(namespace, setname, id);
    final List<String> bins1 = Optional.ofNullable(bins)
        .flatMap(e -> Try.of(() -> e.get()).toJavaOptional())
        .orElse(Collections.emptyList());

    if (DU.isCollectionEmptyOrNull(bins1)) {
      return new BatchRead(key, true);
    }

    return new BatchRead(key, bins1.toArray(new String[0]));
  }

  public static void doBatchReadWithCache(
      final AerospikeClient aerospikeClient,
      final Cache<String, BatchRead> batchReadCache,
      final String namespace,
      final String setName,
      final Collection<String> keys,
      final Consumer<BatchRead> consume
  ) {
    final List<BatchRead> alreadyInCache = new ArrayList<>();
    final List<BatchRead> needToFetch = new ArrayList<>();
    for (final String key : keys) {
      if (batchReadCache.asMap().containsKey(key)) {
        consume.accept(batchReadCache.getIfPresent(key));
      } else {
        needToFetch.add(new BatchRead(new Key(namespace, setName, key), true));
      }
    }
    for (final List<BatchRead> batchReadsParted : Lists.partition(needToFetch,
        AerospikeValues.MAX_BATCH
    )) {
      aerospikeClient.get(null, batchReadsParted);
      for (final BatchRead batchRead : batchReadsParted) {
        consume.accept(batchRead);
        if (Objects.nonNull(batchRead.record)) {
          batchReadCache.put(batchRead.key.userKey.toString(), batchRead);
        }
      }
    }
  }


  public static <V> Map<String, V> doBatchReadWithCacheMapped(
      final AerospikeClient aerospikeClient,
      final Cache<String, V> batchReadCache,
      final String namespace,
      final String setName,
      final Collection<String> keys,
      final Function<BatchRead, V> mapping
  ) {
    final Map<String, V> result = new HashMap<>();
    final List<BatchRead> alreadyInCache = new ArrayList<>();
    final List<BatchRead> needToFetch = new ArrayList<>();
    for (final String key : keys) {
      if (!batchReadCache.asMap().containsKey(key)) {
        needToFetch.add(new BatchRead(new Key(namespace, setName, key), true));
      } else {
        result.put(key, batchReadCache.getIfPresent(key));
      }
    }
    for (final List<BatchRead> batchReads : Lists.partition(needToFetch,
        AerospikeValues.MAX_BATCH
    )) {
      aerospikeClient.get(null, batchReads);
      for (final BatchRead batchRead : batchReads) {
        V apply = mapping.apply(batchRead);
        batchReadCache.put(batchRead.key.userKey.toString(), apply);
        result.put(batchRead.key.userKey.toString(), apply);
      }
    }
    return result;
  }


  public static boolean isContained(final long from, final long to, final long hourBucket) {
    long startBucketMicrosecs = TimeUnit.HOURS.toMicros(hourBucket);
    long endHourBucketMicrosecs = startBucketMicrosecs + TimeUnit.HOURS.toMicros(1) - 1L;
    return (startBucketMicrosecs >= from && endHourBucketMicrosecs <= to) || (
        from >= startBucketMicrosecs && from <= endHourBucketMicrosecs) || (
        to >= startBucketMicrosecs && to <= endHourBucketMicrosecs);
  }

  public static final void deleteAll(
      final AerospikeClient client, final String namespace, final String setName
  ) {
    final Statement statement = new Statement();
    statement.setNamespace(namespace);
    statement.setSetName(setName);
    final RecordSet query = client.query(null, statement);
    while (query.next()) {
      client.delete(null, query.getKey());
    }
  }

}
