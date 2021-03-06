/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.factories;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Objects;

/**
 * Factory class to create default message values.
 *
 * @param <M> Message Value
 */
public abstract class AbstractMessageValueFactory<M extends Writable>
    implements MessageValueFactory<M> {
  /** Message value class */
  private Class<M> messageValueClass;

  /**
   * Get the message value class from the configuration
   *
   * @param conf Configuration
   * @return message value Class
   */
  protected abstract Class<M> extractMessageValueClass(
      ImmutableClassesGiraphConfiguration conf);

  @Override
  public Class<M> getValueClass() {
    return messageValueClass;
  }

  @Override
  public void initialize(ImmutableClassesGiraphConfiguration conf) {
    messageValueClass = extractMessageValueClass(conf);
  }

  @Override public M newInstance() {
    return WritableUtils.createWritable(messageValueClass);
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("messageValueClass", messageValueClass)
        .toString();
  }
}
