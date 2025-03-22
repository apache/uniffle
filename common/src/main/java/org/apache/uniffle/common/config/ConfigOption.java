/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.common.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A {@code ConfigOption} describes a configuration parameter. It encapsulates the configuration
 * key, deprecated older versions of the key, and an optional default value for the configuration
 * parameter.
 *
 * <p>It is built via the {@code ConfigOptions} class. Once created, a config option is immutable.
 *
 * @param <T> The type of value associated with the configuration option.
 */
public class ConfigOption<T> {
  static final FallbackKey[] EMPTY = new FallbackKey[0];
  static final String EMPTY_DESCRIPTION = "";

  /** The current key for that config option. */
  private final String key;

  /** The default value for this option. */
  private final T defaultValue;

  /** The description for this option. */
  private final String description;

  /** Type of the value that this ConfigOption describes. */
  private final Class<?> clazz;

  /** The function which convert the input data to desired type */
  private final Function<Object, T> converter;

  /** The list of deprecated keys, in the order to be checked. */
  private final FallbackKey[] fallbackKeys;

  /**
   * Creates a new config option with fallback keys.
   *
   * @param key The current key for that config option
   * @param clazz describes type of the ConfigOption, see description of the clazz field
   * @param description Description for that option
   * @param defaultValue The default value for this option
   * @param converter The method which convert the input data to desired type
   */
  ConfigOption(
      String key,
      Class<?> clazz,
      String description,
      T defaultValue,
      Function<Object, T> converter,
      FallbackKey... fallbackKeys) {
    this.key = Objects.requireNonNull(key);
    this.description = description;
    this.defaultValue = defaultValue;
    this.clazz = Objects.requireNonNull(clazz);
    this.converter = Objects.requireNonNull(converter);
    this.fallbackKeys = fallbackKeys == null || fallbackKeys.length == 0 ? EMPTY : fallbackKeys;
  }

  /**
   * Creates a new config option, using this option's key and default value, and adding the given
   * description. The given description is used when generation the configuration documentation.
   *
   * @param description The description for this option.
   * @return A new config option, with given description.
   */
  public ConfigOption<T> withDescription(final String description) {
    return new ConfigOption<>(key, clazz, description, defaultValue, converter, fallbackKeys);
  }

  /**
   * Creates a new config option, using this option's key and default value, and adding the given
   * fallback keys.
   *
   * @param fallbackKeys The fallback keys, in the order in which they should be checked.
   * @return A new config options, with the given fallback keys.
   */
  public ConfigOption<T> withFallbackKeys(String... fallbackKeys) {
    final Stream<FallbackKey> newFallbackKeys =
        Arrays.stream(fallbackKeys).map(FallbackKey::createFallbackKey);
    final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

    // put fallback keys first so that they are prioritized
    final FallbackKey[] mergedAlternativeKeys =
        Stream.concat(newFallbackKeys, currentAlternativeKeys).toArray(FallbackKey[]::new);
    return new ConfigOption<>(
        key, clazz, description, defaultValue, converter, mergedAlternativeKeys);
  }

  /**
   * Creates a new config option, using this option's key and default value, and adding the given
   * deprecated keys.
   *
   * @param deprecatedKeys The deprecated keys, in the order in which they should be checked.
   * @return A new config options, with the given deprecated keys.
   */
  public ConfigOption<T> withDeprecatedKeys(final String... deprecatedKeys) {
    final Stream<FallbackKey> newDeprecatedKeys =
        Arrays.stream(deprecatedKeys).map(FallbackKey::createDeprecatedKey);
    final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

    // put deprecated keys last so that they are de-prioritized
    final FallbackKey[] mergedAlternativeKeys =
        Stream.concat(currentAlternativeKeys, newDeprecatedKeys).toArray(FallbackKey[]::new);
    return new ConfigOption<>(
        key, clazz, description, defaultValue, converter, mergedAlternativeKeys);
  }

  // ------------------------------------------------------------------------

  /**
   * Gets the configuration key.
   *
   * @return The configuration key
   */
  public String key() {
    return key;
  }

  /**
   * Checks if this option has a default value.
   *
   * @return True if it has a default value, false if not.
   */
  public boolean hasDefaultValue() {
    return defaultValue != null;
  }

  /**
   * Returns the default value, or null, if there is no default value.
   *
   * @return The default value, or null.
   */
  public T defaultValue() {
    return defaultValue;
  }

  /**
   * Gets the fallback keys, in the order to be checked.
   *
   * @return The option's fallback keys.
   */
  public Iterable<FallbackKey> fallbackKeys() {
    return (fallbackKeys == EMPTY) ? Collections.emptyList() : Arrays.asList(fallbackKeys);
  }

  /**
   * Returns the class of value.
   *
   * @return The option value class
   */
  public Class<?> getClazz() {
    return clazz;
  }

  /**
   * Returns the description of this option.
   *
   * @return The option's description.
   */
  public String description() {
    return description;
  }

  /**
   * The method which convert the input data to desired type
   *
   * @param v the input value
   * @param clazz the desired type
   * @return the value for desired type
   */
  public T convertValue(Object v, Class<?> clazz) {
    return converter.apply(v);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && o.getClass() == this.getClass()) {
      ConfigOption<?> that = (ConfigOption<?>) o;
      return this.key.equals(that.key)
          && (this.defaultValue == null
              ? that.defaultValue == null
              : (that.defaultValue != null && this.defaultValue.equals(that.defaultValue)));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, defaultValue);
  }

  @Override
  public String toString() {
    return String.format("Key: '%s' , default: %s", key, defaultValue);
  }
}
