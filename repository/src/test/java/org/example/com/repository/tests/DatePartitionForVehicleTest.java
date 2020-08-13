package org.example.com.repository.tests;

import com.aerospike.client.AerospikeClient;
import java.util.concurrent.TimeUnit;
import org.example.com.repository.DatePartitionForVehicleIndexImpl;
import org.example.com.repository.IDatePartitionForVehicleIndex;
import org.example.com.repository.data.PartedKeyValues;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DatePartitionForVehicleTest {

  private final AerospikeClient client;
  private final IDatePartitionForVehicleIndex repository;
  private final DateTime dateTime = new DateTime(2020, 2, 12, 15, 12, DateTimeZone.UTC);

  public DatePartitionForVehicleTest() {
    client = AerospikeSingletonTest.CLIENT;
    this.repository = new DatePartitionForVehicleIndexImpl(client);
  }

  @BeforeClass
  public void deleteAll() {
    TestUtils.deleteAll(client);
  }

  @Test(priority = 10)
  public void readEmpty() {
    final PartedKeyValues operatorsOnDatePart = repository.getVehiclesOnDatePart(
        TestUtils.dateTimeToMicros(dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );
    Assert.assertTrue(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
  }

  @Test(priority = 11)
  public void writeSingle() {
    final VehicleTestData vehicleTestData = DatePartitionForVehicleTest.simpleData(dateTime);
    repository.writeVehicleOnDatePart(vehicleTestData.ts, vehicleTestData.id);
    repository.invalidateCache(vehicleTestData.ts);
  }

  @Test(priority = 12)
  public void readSingle() {
    final PartedKeyValues operatorsOnDatePart = repository.getVehiclesOnDatePart(
        TestUtils.dateTimeToMicros(dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );
    Assert.assertFalse(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertFalse(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertFalse(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
  }

  @Test(priority = 13)
  public void readEmptyAfterWrite() {
    final PartedKeyValues operatorsOnDatePart = repository.getVehiclesOnDatePart(
        TestUtils.dateTimeToMicros(dateTime.plusDays(2)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(3))
    );
    Assert.assertTrue(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
  }


  @Test(priority = 14)
  public void write2More() {
    final VehicleTestData vehicleTestData1 = DatePartitionForVehicleTest.simpleData(dateTime.plusHours(
        1));
    repository.writeVehicleOnDatePart(vehicleTestData1.ts, vehicleTestData1.id);
    repository.invalidateCache(vehicleTestData1.ts);
    final VehicleTestData vehicleTestData2 = DatePartitionForVehicleTest.simpleData(dateTime.plusDays(
        1));
    repository.writeVehicleOnDatePart(vehicleTestData2.ts, vehicleTestData2.id);
    repository.invalidateCache(vehicleTestData2.ts);
  }

  @Test(priority = 15)
  public void readMultiple3() {
    final PartedKeyValues operatorsOnDatePart = repository.getVehiclesOnDatePart(
        TestUtils.dateTimeToMicros(dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );
    Assert.assertEquals(operatorsOnDatePart.getHours().size(), 3);
    System.out.println(operatorsOnDatePart);
  }

  public static VehicleTestData simpleData(final DateTime dateTime) {
    return new VehicleTestData("1", TimeUnit.MILLISECONDS.toMicros(dateTime.getMillis()));
  }

  private static class VehicleTestData {

    private String id;
    private long ts;

    public VehicleTestData(final String id, final long ts) {
      this.id = id;
      this.ts = ts;
    }

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public long getTs() {
      return ts;
    }

    public void setTs(final long ts) {
      this.ts = ts;
    }

    @Override
    public String toString() {
      return "OperatorTestData{" + "id='" + id + '\'' + ", ts=" + ts + '}';
    }
  }
}
