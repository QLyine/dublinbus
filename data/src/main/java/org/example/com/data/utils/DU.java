package org.example.com.data.utils;

import com.google.common.base.Supplier;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Data utilities
 */
public class DU {

  public static <T> boolean isCollectionEmptyOrNull(final Collection<T> collection) {
    return Objects.isNull(collection) || collection.isEmpty();
  }

  public static <K, V> boolean isMapEmptyOrNull(final Map<K, V> map) {
    return Objects.isNull(map) || map.isEmpty();
  }


  public static <K, V, C extends Collection<V>> void appendToCollection(
      final Map<K, C> map, final K k, final V v, final Supplier<C> supplier
  ) {
    map.computeIfPresent(k, (k1, vs) -> {
      vs.add(v);
      return vs;
    });
    map.computeIfAbsent(k, kk -> {
      final C collection = supplier.get();
      collection.add(v);
      return collection;
    });
  }

}
