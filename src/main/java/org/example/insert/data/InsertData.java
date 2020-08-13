package org.example.insert.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.example.com.data.VehicleData;
import org.example.com.repository.IVehiclesRepository;
import org.example.com.repository.Utils;
import org.example.com.repository.VehiclesRepositoryImpl;
import org.example.eda.ReadCsv;

public class InsertData {

  public static void main(final String[] args) throws InterruptedException {
    System.out.println("Connecting to aerospike 127.0.0.1:300");
    final IVehiclesRepository repository = new VehiclesRepositoryImpl("127.0.0.1", 3000);

    System.out.println("Starting to write ....");
    final ExecutorService[] executors = new ExecutorService[
        Runtime.getRuntime().availableProcessors() * 4];
    for (int i = 0; i < executors.length; i++) {
      executors[i] = Executors.newSingleThreadScheduledExecutor();
    }
    final List<VehicleData> collect = Arrays.stream(args).flatMap(e -> {
      try {
        return ReadCsv.readFromFileStream(e);
      } catch (final IOException ioException) {
        ioException.printStackTrace();
        System.exit(1);
      }
      return Stream.empty();
    }).parallel().map(e -> new VehicleData(
        e.getTimestamp(),
        e.getLon(),
        e.getLat(),
        e.getVehicleId(),
        e.getOperator(),
        e.getStopId(),
        e.isStop()
    )).collect(Collectors.toList());
    final CountDownLatch countDownLatch = new CountDownLatch(collect.size());
    collect.parallelStream().forEach(e -> {
      final String vehicleId = e.getVehicleId();
      final long hours = TimeUnit.MICROSECONDS.toHours(e.getTs());
      final String timedKey = Utils.createTimedKey(hours, vehicleId);
      final int i = Math.abs(timedKey.hashCode() % executors.length);
      executors[i].submit(() -> {
        repository.writeData(e);
        countDownLatch.countDown();
      });
    });

    countDownLatch.await();
    System.out.println("Wrote all ... ");

    System.out.println("Shutting Down!");
    for (final ExecutorService executor : executors) {
      executor.shutdown();
    }

  }

}
