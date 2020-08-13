package org.example.com.repository.tests;

import com.aerospike.client.AerospikeClient;

public class AerospikeSingletonTest {

  public static final AerospikeClient CLIENT = new AerospikeClient("127.0.0.1", 3000);

}
