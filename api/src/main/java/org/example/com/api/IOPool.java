package org.example.com.api;

import java.util.concurrent.TimeUnit;
import org.example.com.repository.ScalingThreadExecutor;

public class IOPool {

  public static final ScalingThreadExecutor EXECUTORS = new ScalingThreadExecutor(Runtime.getRuntime()
      .availableProcessors(), Runtime.getRuntime().availableProcessors() * 2, 1, TimeUnit.MINUTES);


}
