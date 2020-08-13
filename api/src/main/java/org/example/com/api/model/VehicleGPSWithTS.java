package org.example.com.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class VehicleGPSWithTS {

  private long tsMicro;
  private float lon;
  private float lat;

}
