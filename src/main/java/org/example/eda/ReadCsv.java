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

  public static Stream<CsvRow> readFromFileStream(final String fileLocation) throws IOException {
    return Files.lines(Paths.get(fileLocation)).map(e -> e.split(",")).map(e -> map(e));
  }

  public static List<CsvRow> readFromFile(final String fileLocation) throws IOException {
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

