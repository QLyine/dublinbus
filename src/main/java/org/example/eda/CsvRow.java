package org.example.eda;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class CsvRow {

  private long timestamp;
  private String lineId;
  private int direction;
  private String journeyPatternId;
  private String timeFrame;
  private String vehicleJourneyId;
  private String operator;
  private boolean congestion;
  // GPS
  private float lon;
  private float lat;
  private int dalayInSeconds;
  private String blockId;
  private String vehicleId;
  private String stopId;
  private boolean stop;


}
