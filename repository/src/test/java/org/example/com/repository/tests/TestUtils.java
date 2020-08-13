package org.example.com.repository.tests;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.google.common.collect.ImmutableSet;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;

public class TestUtils {

  private static final ImmutableSet<String> SETS_TO_DELETE = ImmutableSet.of("vehicByOpPart",
      "datepart",
      "datevehiclepart",
      "vehicles"
  );

  public static void deleteAll(final AerospikeClient client) {
    TestUtils.SETS_TO_DELETE.forEach(e -> TestUtils.deleteAll(client, "test", e));
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

  public static final long dateTimeToMicros(final DateTime dateTime) {
    return TimeUnit.MILLISECONDS.toMicros(dateTime.getMillis());
  }
}
