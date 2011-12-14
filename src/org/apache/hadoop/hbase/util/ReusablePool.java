/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.hadoop.hbase.util.PoolMap.Pool;

/**
 * The <code>ReusablePool</code> represents a {@link SharedMap.Pool} that builds
 * on the {@link LinkedList} class. It essentially allows resources to be
 * checked out, at which point it is removed from this pool. When the resource
 * is no longer required, it should be returned to the pool in order to be
 * reused.
 * 
 * <p>
 * If {@link #maxSize} is set to {@link Integer#MAX_VALUE}, then the size of the
 * pool is unbounded. Otherwise, it caps the number of consumers that can check
 * out a resource from this pool to the (non-zero positive) value specified in
 * {@link #maxSize}.
 * </p>
 * 
 * @author Karthick Sankarachary
 * 
 * @param <R>
 *          the type of the resource
 */
@SuppressWarnings("serial")
public class ReusablePool<R> extends LinkedList<R> implements Pool<R> {
  private int maxSize;

  public ReusablePool(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public R get() {
    return poll();
  }

  @Override
  public R put(R resource) {
    if (size() < maxSize) {
      add(resource);
    }
    return null;
  }

  @Override
  public Collection<R> values() {
    return this;
  }
}