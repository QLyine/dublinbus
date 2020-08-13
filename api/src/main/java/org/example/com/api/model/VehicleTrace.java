package org.example.com.api.model;

import java.util.LinkedList;
import java.util.List;
import lombok.Data;

@Data
public class VehicleTrace {

  public VehicleTrace(final String id) {
    this.id = id;
    this.trace = new LinkedList<>();
  }

  private String id;
  private List<VehicleGPSWithTS> trace;

}
