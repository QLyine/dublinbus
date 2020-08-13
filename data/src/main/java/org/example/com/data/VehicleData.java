package org.example.com.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VehicleData implements Comparable<VehicleData> {

  private long ts;
  private float lon;
  private float lat;
  private String vehicleId;
  private String operatorId;
  private String stopId;
  private boolean stopped;

  @Override
  public int compareTo(final VehicleData o) {
    final int compare = Long.compare(ts, o.ts);
    if (compare == 0) {
      return StringUtils.compare(vehicleId, o.vehicleId);
    }
    return compare;
  }
}
