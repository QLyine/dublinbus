package org.example.com.repository;

import com.aerospike.client.policy.WritePolicy;

public class WritePolicies {

  public static WritePolicy getSendKeyPolicy() {
    final WritePolicy sendKeyPolicy = new WritePolicy();
    sendKeyPolicy.sendKey = true;
    return sendKeyPolicy;
  }

}
