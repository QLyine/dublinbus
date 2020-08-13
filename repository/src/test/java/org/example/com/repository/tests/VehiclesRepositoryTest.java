package org.example.com.repository.tests;

import com.aerospike.client.AerospikeClient;
import java.util.NavigableSet;
import java.util.Set;
import org.example.com.data.utils.VehicleData;
import org.example.com.repository.IVehiclesRepository;
import org.example.com.repository.VehiclesRepositoryImpl;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class VehiclesRepositoryTest {

  private final AerospikeClient client;
  private final IVehiclesRepository repository;
  private final DateTime dateTime = new DateTime(2020, 3, 12, 15, 12, DateTimeZone.UTC);

  public VehiclesRepositoryTest() {
    client = AerospikeSingletonTest.CLIENT;
    this.repository = new VehiclesRepositoryImpl(client);
  }

  @BeforeMethod
  public void deleteAll() {
    repository.deleteAllData();
    repository.invalidateAllCache();
  }

  @Test()
  public void readWriteSingleOperator() {
    final Set<String> emptyOperator = repository.getOperators(TestUtils.dateTimeToMicros(dateTime.minusDays(
        1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );
    Assert.assertTrue(emptyOperator.isEmpty());

    writeAndCreateSampleData("o1", "v1", dateTime);
    final Set<String> oneOperator = repository.getOperators(TestUtils.dateTimeToMicros(dateTime.minusDays(
        1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );

    Assert.assertFalse(oneOperator.isEmpty());
    Assert.assertEquals(oneOperator.size(), 1);

  }

  @Test()
  public void readWrite10OperatorsInOneHour() {
    for (int i = 0; i < 100; i++) {
      writeAndCreateSampleData("o" + i, "v1", dateTime.plus(i));
    }
    final Set<String> operators = repository.getOperators(TestUtils.dateTimeToMicros(dateTime),
        TestUtils.dateTimeToMicros(dateTime.plusMinutes(5))
    );

    Assert.assertFalse(operators.isEmpty());
    Assert.assertEquals(operators.size(), 100);

    final Set<String> fiftyOperators = repository.getOperators(TestUtils.dateTimeToMicros(dateTime.minus(
        25)),
        TestUtils.dateTimeToMicros(dateTime.plus(50 - 1))
    );

    Assert.assertFalse(fiftyOperators.isEmpty());
    Assert.assertEquals(fiftyOperators.size(), 50);
  }

  @Test()
  public void readWrite3Operator() {
    writeAndCreateSampleData("o31", "v1", dateTime.plusHours(1));
    writeAndCreateSampleData("o32", "v3", dateTime.plusHours(2));
    writeAndCreateSampleData("o33", "v3", dateTime.plusHours(3));
    final Set<String> threeOperator = repository.getOperators(TestUtils.dateTimeToMicros(dateTime.minusDays(
        1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );

    Assert.assertFalse(threeOperator.isEmpty());
    Assert.assertEquals(threeOperator.size(), 3);

  }

  @Test()
  public void readVehicleIdOfOperator() {
    writeAndCreateSampleData("o31", "v1", dateTime.plusHours(1));
    writeAndCreateSampleData("o31", "v3", dateTime.plusHours(2));
    writeAndCreateSampleData("o33", "v3", dateTime.plusHours(3));

    final NavigableSet<VehicleData> operatorWith2VehicleIds = repository.getVehiclesOfOperator(
        TestUtils.dateTimeToMicros(dateTime),
        TestUtils.dateTimeToMicros(dateTime.plusHours(2)),
        "o31"
    );

    Assert.assertFalse(operatorWith2VehicleIds.isEmpty());
    Assert.assertEquals(operatorWith2VehicleIds.size(), 2);
    Assert.assertTrue(operatorWith2VehicleIds.stream()
        .allMatch(e -> e.getOperatorId().equals("o31")));

    final NavigableSet<VehicleData> twoVehicleIds = repository.getVehiclesDataWithId(TestUtils.dateTimeToMicros(
        dateTime), TestUtils.dateTimeToMicros(dateTime.plusHours(3)), "v3");

    Assert.assertFalse(twoVehicleIds.isEmpty());
    Assert.assertEquals(twoVehicleIds.size(), 2);
    Assert.assertTrue(twoVehicleIds.stream().allMatch(e -> e.getVehicleId().equals("v3")));
  }


  @Test()
  public void readWrite60perators() {
    for (int i = 0; i < 60; i++) {
      writeAndCreateSampleData("o" + i, "v1", dateTime.plusHours(i));
    }
    final Set<String> threeOperator = repository.getOperators(TestUtils.dateTimeToMicros(dateTime.minusDays(
        10)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(10))
    );

    Assert.assertFalse(threeOperator.isEmpty());
    Assert.assertEquals(threeOperator.size(), 60);
  }

  public void writeAndCreateSampleData(
      final String operatorId, final String vehicleId, final DateTime dateTime
  ) {
    repository.writeData(VehiclesRepositoryTest.createSampleData(operatorId, vehicleId, dateTime));
  }

  public void writeData(final VehicleData vehicleData) {
    repository.writeData(vehicleData);
  }

  public static VehicleData createSampleData(
      final String operatorId, final String vehicleId, final DateTime dateTime
  ) {
    return new VehicleData(TestUtils.dateTimeToMicros(dateTime), vehicleId, operatorId, "", false);
  }
}
