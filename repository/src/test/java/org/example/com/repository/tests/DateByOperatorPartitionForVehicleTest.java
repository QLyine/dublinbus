package org.example.com.repository.tests;

import com.aerospike.client.AerospikeClient;
import java.util.concurrent.TimeUnit;
import org.example.com.repository.DateByOperatorPartForVehicleImpl;
import org.example.com.repository.IDateByOperatorPartForVehicleIndex;
import org.example.com.repository.data.PartedKeyValues;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DateByOperatorPartitionForVehicleTest {

  private final AerospikeClient client;
  private final IDateByOperatorPartForVehicleIndex repository;
  private final DateTime dateTime = new DateTime(2020, 2, 12, 15, 12, DateTimeZone.UTC);

  public DateByOperatorPartitionForVehicleTest() {
    client = AerospikeSingletonTest.CLIENT;
    this.repository = new DateByOperatorPartForVehicleImpl(client);
  }

  @BeforeClass
  public void deleteAll() {
    TestUtils.deleteAll(client);
  }

  @Test(priority = 0)
  public void readEmpty() {
    final PartedKeyValues operatorsOnDatePart = repository.getVehiclesOnDateOperatorPart(TestUtils.dateTimeToMicros(
        dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1)),
        "a"
    );
    Assert.assertTrue(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
  }

  @Test(priority = 1)
  public void writeSingle() {
    final OperatorVehicleTestData data = DateByOperatorPartitionForVehicleTest.simpleData("2",
        "22",
        dateTime
    );
    repository.writeVehicleOnDatePartOperator(data.ts, data.operatorId, data.vehicleId);
    repository.invalidateCache(data.ts, data.operatorId);
  }

  @Test(priority = 2)
  public void readSingleWrongOperator() {
    final PartedKeyValues operatorsOnDatePart = repository.getVehiclesOnDateOperatorPart(TestUtils.dateTimeToMicros(
        dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1)),
        "a"
    );
    Assert.assertTrue(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
  }


  @Test(priority = 3)
  public void readSingleOperator() {
    final PartedKeyValues operatorsOnDatePart = repository.getVehiclesOnDateOperatorPart(TestUtils.dateTimeToMicros(
        dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1)),
        "2"
    );
    Assert.assertFalse(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertFalse(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertFalse(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
    Assert.assertEquals(operatorsOnDatePart.getHours().size(), 1);
  }

  @Test(priority = 4)
  public void write2More() {
    writeData("2", "22", dateTime.plusHours(1));
    writeData("3", "33", dateTime.plusMinutes(1));
  }

  @Test(priority = 5)
  public void read3Records() {
    final PartedKeyValues operatorsOnDatePart = repository.getVehiclesOnDateOperatorPart(TestUtils.dateTimeToMicros(
        dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1)),
        "2"
    );
    Assert.assertFalse(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertFalse(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertFalse(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
    Assert.assertEquals(operatorsOnDatePart.getHours().size(), 2);
    Assert.assertEquals(operatorsOnDatePart.getValuesOnEachHour().size(), 2);
    Assert.assertEquals(operatorsOnDatePart.getTimeInHoursByKeyValue().size(), 1);

    final PartedKeyValues operatorsOnDatePartId3 = repository.getVehiclesOnDateOperatorPart(
        TestUtils.dateTimeToMicros(dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1)),
        "3"
    );
    Assert.assertFalse(operatorsOnDatePartId3.getHours().isEmpty());
    Assert.assertFalse(operatorsOnDatePartId3.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertFalse(operatorsOnDatePartId3.getValuesOnEachHour().isEmpty());
    Assert.assertEquals(operatorsOnDatePartId3.getHours().size(), 1);
    Assert.assertEquals(operatorsOnDatePartId3.getValuesOnEachHour().size(), 1);
    Assert.assertEquals(operatorsOnDatePartId3.getTimeInHoursByKeyValue().size(), 1);
  }

  private void writeData(final String operatorId, final String vehicleId, final DateTime dateTime) {
    final OperatorVehicleTestData data = DateByOperatorPartitionForVehicleTest.simpleData(operatorId,
        vehicleId,
        dateTime
    );
    repository.writeVehicleOnDatePartOperator(data.ts, data.operatorId, data.vehicleId);
    repository.invalidateCache(data.ts, data.operatorId);
  }

  public static OperatorVehicleTestData simpleData(
      final String operatorId, final String vehicleId, final DateTime dateTime
  ) {
    return new OperatorVehicleTestData(operatorId,
        vehicleId,
        TimeUnit.MILLISECONDS.toMicros(dateTime.getMillis())
    );
  }

  private static class OperatorVehicleTestData {

    private String operatorId;
    private final String vehicleId;
    private long ts;

    public OperatorVehicleTestData(
        final String operatorId, final String vehicleId, final long ts
    ) {
      this.operatorId = operatorId;
      this.vehicleId = vehicleId;
      this.ts = ts;
    }

    public String getOperatorId() {
      return operatorId;
    }

    public void setOperatorId(final String operatorId) {
      this.operatorId = operatorId;
    }

    public long getTs() {
      return ts;
    }

    public void setTs(final long ts) {
      this.ts = ts;
    }

    public String getVehicleId() {
      return vehicleId;
    }

    @Override
    public String toString() {
      return "OperatorTestData{" + "id='" + operatorId + '\'' + ", ts=" + ts + '}';
    }
  }
}
