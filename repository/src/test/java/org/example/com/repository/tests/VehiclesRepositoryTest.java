package org.example.com.repository.tests;

import com.aerospike.client.AerospikeClient;
import java.util.NavigableSet;
import java.util.Set;
import org.example.com.data.VehicleData;
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
    testOperatorsGet(dateTime, dateTime, 1);
    testOperatorsGet(dateTime.plus(1), dateTime.plus(1), 1);
    testOperatorsGet(dateTime, dateTime.plus(59), 60);
    testOperatorsGet(dateTime.minus(10), dateTime.plus(9), 10);
    testOperatorsGet(dateTime.minus(10), dateTime.plus(100), 100);
    testOperatorsGet(dateTime.plus(50), dateTime.plus(150), 50);
  }

  @Test()
  public void readWrite10OperatorsInOneDay() {
    for (int i = 0; i < 10; i++) {
      writeAndCreateSampleData("o" + i, "v1", dateTime.plusHours(i));
    }
    testOperatorsGet(dateTime.plus(1), dateTime.plus(1), 0);
    testOperatorsGet(dateTime, dateTime, 1);
    testOperatorsGet(dateTime, dateTime.plusHours(1), 2);
    testOperatorsGet(dateTime, dateTime.plusHours(2), 3);
    testOperatorsGet(dateTime, dateTime.plusHours(4).minus(1), 4);
    testOperatorsGet(dateTime, dateTime.plusHours(4), 5);
    testOperatorsGet(dateTime.plusHours(1), dateTime.plusHours(4).minus(1), 3);
  }

  public void testOperatorsGet(final DateTime from, final DateTime to, final int expectedSize) {
    final Set<String> set = repository.getOperators(TestUtils.dateTimeToMicros(from),
        TestUtils.dateTimeToMicros(to)
    );
    Assert.assertEquals(set.size(), expectedSize);
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

  private void testNVehicleOfOperator(
      final DateTime from, final DateTime to, final String operator, final int n
  ) {
    final NavigableSet<VehicleData> vehiclesOfOperator = repository.getVehiclesOfOperator(TestUtils.dateTimeToMicros(
        from), TestUtils.dateTimeToMicros(to), operator);

    Assert.assertEquals(vehiclesOfOperator.size(), n);
  }


  @Test()
  public void readWrite60OperatorsPerHour() {
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


  @Test()
  public void readWriteVehicleStopped() {
    for (int i = 0; i < 10; i++) {
      writeAndCreateSampleData("o", "v1", dateTime.plus(i), true);
    }
    for (int i = 5; i < 10; i++) {
      writeAndCreateSampleData("o", "v2", dateTime.plus(i), true);
    }
    for (int i = 10; i < 20; i++) {
      writeAndCreateSampleData("o", "v1", dateTime.plus(i), false);
    }

    queryStoppedAndAssert(dateTime, dateTime, "o", 1);
    queryStoppedAndAssert(dateTime.plus(5), dateTime.plus(7), "o", 2);
    queryStoppedAndAssert(dateTime, dateTime.plus(9), "o", 2);

    queryStoppedAndAssert(dateTime, dateTime.plus(11), "o", 1);

  }

  @Test()
  public void readWriteVehicleStoppedInvalidatingCache() {
    for (int i = 0; i < 10; i++) {
      writeAndCreateSampleData("o", "v1", dateTime.plus(i), true);
    }
    for (int i = 5; i < 10; i++) {
      writeAndCreateSampleData("o", "v2", dateTime.plus(i), true);
    }
    for (int i = 10; i < 20; i++) {
      writeAndCreateSampleData("o", "v1", dateTime.plus(i), false);
    }
    repository.invalidateAllCache();

    queryStoppedAndAssert(dateTime, dateTime, "o", 1);
    queryStoppedAndAssert(dateTime.plus(5), dateTime.plus(7), "o", 2);
    queryStoppedAndAssert(dateTime, dateTime.plus(9), "o", 2);

    queryStoppedAndAssert(dateTime, dateTime.plus(11), "o", 1);

  }

  public void queryStoppedAndAssert(
      final DateTime from, final DateTime to, final String operator, final int assertSize
  ) {
    final Set<String> ids = repository.getVehiclesStoppedOfOperator(TestUtils.dateTimeToMicros(from),
        TestUtils.dateTimeToMicros(to),
        operator
    );

    Assert.assertEquals(ids.size(), assertSize);
  }

  public void writeAndCreateSampleData(
      final String operatorId, final String vehicleId, final DateTime dateTime
  ) {
    repository.writeData(VehiclesRepositoryTest.createSampleData(operatorId, vehicleId, dateTime));
  }

  public void writeAndCreateSampleData(
      final String operatorId,
      final String vehicleId,
      final DateTime dateTime,
      final boolean stopped
  ) {
    repository.writeData(VehiclesRepositoryTest.createSampleDataWithStop(operatorId,
        vehicleId,
        dateTime,
        stopped
    ));
  }

  public void writeData(final VehicleData vehicleData) {
    repository.writeData(vehicleData);
  }

  public static VehicleData createSampleData(
      final String operatorId, final String vehicleId, final DateTime dateTime
  ) {
    return new VehicleData(TestUtils.dateTimeToMicros(dateTime),
        0f,
        0f,
        vehicleId,
        operatorId,
        "",
        false
    );
  }

  public static VehicleData createSampleDataWithStop(
      final String operatorId,
      final String vehicleId,
      final DateTime dateTime,
      final boolean stopped
  ) {
    return new VehicleData(TestUtils.dateTimeToMicros(dateTime),
        0f,
        0f,
        vehicleId,
        operatorId,
        "suid",
        stopped
    );
  }
}
