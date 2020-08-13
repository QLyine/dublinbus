package org.example.eda;

import static java.util.stream.Collectors.toList;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@SuppressWarnings("ALL")
public class ReadCsv {

  public static void main(String[] args) throws IOException {
    Stream<CsvRow> csvRowStream = Arrays.asList(
        "/home/qlyine/Downloads/siri.20121106.csv",
        "/home/qlyine/Downloads/siri.20121107.csv",
        "/home/qlyine/Downloads/siri.20121108.csv"
    ).stream().parallel().flatMap(e -> {
      try {
        return readFromFileStream(e);
      } catch (IOException ioException) {
        ioException.printStackTrace();
        return Stream.empty();
      }
    });

    Supplier<List<CsvRow>> listSupplier = () -> new ArrayList<>();
    Map<String, List<CsvRow>> vehicleByHour = new ConcurrentHashMap<>();
    csvRowStream.forEach(e -> {
      final long hours = TimeUnit.MICROSECONDS.toHours(e.getTimestamp());
      final String key = String.format("%d:%s", hours, e.getVehicleId());
      appendToCollection(vehicleByHour, key, e, listSupplier);
    });

    Optional<Entry<String, List<CsvRow>>> max = vehicleByHour.entrySet()
        .stream()
        .max((o1, o2) -> Integer.compare(o1.getValue().size(), o2.getValue().size()));

    System.out.println(max.get().getValue().size());

    /*Map<String, List<CsvRow>> operatorsVehicleIds = new HashMap<>();
    Supplier<List<CsvRow>> listSupplier = () -> new ArrayList<>();
    Supplier<Set<String>> hashSupplier = () -> new HashSet<>();
    Map<Integer, Set<String>> timeOperators = new HashMap<>();
    final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    final AtomicLong max = new AtomicLong(Long.MAX_VALUE);
    concat.forEach(csvRow -> {
      String key = String.format("%s:%s:%s",
          csvRow.getTimeFrame(),
          csvRow.getOperator(),
          csvRow.getVehicleId()
      );
      appendToCollection(operatorsVehicleIds, key, csvRow, listSupplier);
      final int hours = (int) TimeUnit.MICROSECONDS.toHours(csvRow.getTimestamp());
      appendToCollection(operatorsVehicleIds, key, csvRow, listSupplier);
      min.set(Math.min(csvRow.getTimestamp(), min.get()));
      max.set(Math.max(csvRow.getTimestamp(), max.get()));
      final String value = String.format("%s:%s", csvRow.getTimeFrame(), csvRow.getOperator());
      appendToCollection(timeOperators, hours, value, hashSupplier);
    });

    for (final Entry<Integer, Set<String>> integerSetEntry : timeOperators.entrySet()) {
      final Integer key = integerSetEntry.getKey();
      final long instant = TimeUnit.HOURS.toMillis(key);
      final DateTime dateTime = new DateTime(instant);
      System.out.println(String.format("%s - %s - %s",
          dateTime,
          integerSetEntry.getValue().size(),
          integerSetEntry.getValue()
      ));
    }

    /*
    operatorsVehicleIds.entrySet()
        .stream()
        .sorted((e1, e2) -> -1 * Integer.compare(e1.getValue().size(), e2.getValue().size()))
        .limit(1)
        .forEach(e -> {
          String key = e.getKey();
          System.out.println(key + " " + e.getValue().size());
          e.getValue()
              .stream()
              .sorted((o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp()))
              .forEach(v -> System.out.println(v));
        });
     */
  }

  private static <K, V, C extends Collection<V>> void appendToCollection(
      Map<K, C> map, K k, V v, Supplier<C> supplier
  ) {
    map.computeIfAbsent(k, kk -> {
      C collection = supplier.get();
      collection.add(v);
      return collection;
    });
    map.computeIfPresent(k, (k1, vs) -> {
      vs.add(v);
      return vs;
    });
  }

  private static Stream<CsvRow> readFromFileStream(final String fileLocation) throws IOException {
    return Files.lines(Paths.get(fileLocation)).map(e -> e.split(",")).map(e -> map(e));
  }

  private static List<CsvRow> readFromFile(final String fileLocation) throws IOException {
    return Files.lines(Paths.get(fileLocation))
        .map(e -> e.split(","))
        .map(e -> map(e))
        .collect(toList());
  }

  private static CsvRow map(final String[] line) {
    final long timeStamp = Long.parseLong(line[0]);
    final String lineId = readPossibleNullString(line[1]);
    final int direction = Integer.parseInt(line[2]);
    final String journeyPatternId = readPossibleNullString(line[3]);
    final String timeFrame = line[4];
    final String vehicleJourneyId = line[5];
    final String operator = line[6];
    final boolean congestion = Integer.parseInt(line[7]) == 0 ? false : true;
    final float lon = Float.parseFloat(line[8]);
    final float lat = Float.parseFloat(line[9]);
    final int delayInSeconds = Integer.parseInt(line[10]);
    final String blockId = line[11];
    final String vehicleId = line[12];
    final String stopId = readPossibleNullString(line[13]);
    final boolean stop = Integer.parseInt(line[14]) == 0 ? false : true;
    return new CsvRow(
        timeStamp,
        lineId,
        direction,
        journeyPatternId,
        timeFrame,
        vehicleJourneyId,
        operator,
        congestion,
        lon,
        lat,
        delayInSeconds,
        blockId,
        vehicleId,
        stopId,
        stop
    );
  }

  private static String readPossibleNullString(String s) {
    return Strings.isNullOrEmpty(s) ? "" : s;
  }

  private static int readInteger(String s, int defaultValue) {
    if (Strings.isNullOrEmpty(s)) {
      return defaultValue;
    }
    return Integer.parseInt(s);
  }
}

