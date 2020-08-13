package org.example.com.repository.tests;

import com.aerospike.client.AerospikeClient;
import java.util.concurrent.TimeUnit;
import org.example.com.repository.DatePartitionForOperatorIndexImpl;
import org.example.com.repository.IDatePartitionForOperatorIndex;
import org.example.com.repository.data.PartedKeyValues;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatePartitionOperatorTest {

  private final AerospikeClient client;
  private final IDatePartitionForOperatorIndex repository;
  private final DateTime dateTime = new DateTime(2020, 2, 12, 15, 12, DateTimeZone.UTC);

  public DatePartitionOperatorTest() {
    client = AerospikeSingletonTest.CLIENT;
    this.repository = new DatePartitionForOperatorIndexImpl(client);
  }

  @Test(priority = 20)
  public void readEmpty() {
    System.out.println("READ_EMPTY");
    final PartedKeyValues operatorsOnDatePart = repository.getOperatorsOnDatePart(
        TestUtils.dateTimeToMicros(dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );
    Assert.assertTrue(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
  }

  @Test(priority = 21)
  public void writeSingle() {
    System.out.println("WRITE_SINGLE");
    final OperatorTestData operatorTestData = DatePartitionOperatorTest.simpleData(dateTime);
    repository.writeOperatorsOnDatePart(operatorTestData.ts, operatorTestData.id);
    repository.invalidateCache(operatorTestData.ts);
  }

  @Test(priority = 22)
  public void readSingle() {
    System.out.println("READ_SINGLE");
    final PartedKeyValues operatorsOnDatePart = repository.getOperatorsOnDatePart(
        TestUtils.dateTimeToMicros(dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );
    Assert.assertFalse(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertFalse(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertFalse(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
  }

  @Test(priority = 23)
  public void readEmptyAfterWrite() {
    System.out.println("READ_EMPTY_AFTER_WRITE");
    final PartedKeyValues operatorsOnDatePart = repository.getOperatorsOnDatePart(
        TestUtils.dateTimeToMicros(dateTime.plusDays(2)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(3))
    );
    Assert.assertTrue(operatorsOnDatePart.getHours().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getTimeInHoursByKeyValue().isEmpty());
    Assert.assertTrue(operatorsOnDatePart.getValuesOnEachHour().isEmpty());
  }


  @Test(priority = 24)
  public void write2More() {
    System.out.println("WRITE_2_MORE");
    final OperatorTestData operatorTestData1 = DatePartitionOperatorTest.simpleData(dateTime.plusHours(
        1));
    repository.writeOperatorsOnDatePart(operatorTestData1.ts, operatorTestData1.id);
    repository.invalidateCache(operatorTestData1.ts);
    final OperatorTestData operatorTestData2 = DatePartitionOperatorTest.simpleData(dateTime.plusDays(
        1));
    repository.writeOperatorsOnDatePart(operatorTestData2.ts, operatorTestData2.id);
    repository.invalidateCache(operatorTestData2.ts);
  }

  @Test(priority = 25)
  public void readMultiple3() {
    System.out.println("READ_MULTIPLE_3");
    final PartedKeyValues operatorsOnDatePart = repository.getOperatorsOnDatePart(
        TestUtils.dateTimeToMicros(dateTime.minusDays(1)),
        TestUtils.dateTimeToMicros(dateTime.plusDays(1))
    );
    Assert.assertEquals(operatorsOnDatePart.getHours().size(), 3);
    System.out.println(operatorsOnDatePart);
  }

  public static OperatorTestData simpleData(final DateTime dateTime) {
    return new OperatorTestData("1", TimeUnit.MILLISECONDS.toMicros(dateTime.getMillis()));
  }

  private static class OperatorTestData {

    private String id;
    private long ts;

    public OperatorTestData(final String id, final long ts) {
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
