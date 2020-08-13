package org.example.com.repository.data;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PartedKeyValues {

  // OrderedInHours ASC ORD
  IntList hours;
  List<List<String>> valuesOnEachHour;
  Map<String, IntList> timeInHoursByKeyValue;

}
