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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.hbase.util.PoolMap.Pool;

/**
 * The <code>RoundRobinPool</code> represents a {@link SharedMap.Pool}, which
 * stores its resources in an {@link ArrayList}. It load-balances access to its
 * resources by returning a different resource every time a given key is looked
 * up.
 * 
 * <p>
 * If {@link #maxSize} is set to {@link Integer#MAX_VALUE}, then the size of the
 * pool is unbounded. Otherwise, it caps the number of resources in this pool to
 * the (non-zero positive) value specified in {@link #maxSize}.
 * </p>
 * 
 * @author Karthick Sankarachary
 * 
 * @param <R>
 *          the type of the resource
 * 
 */
@SuppressWarnings("serial")
class RoundRobinPool<R> extends CopyOnWriteArrayList<R> implements Pool<R> {
    private int maxSize;
    private int nextResource = 0;

    public RoundRobinPool(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    public R put(R resource) {
      if (size() < maxSize) {
        add(resource);
      }
      return null;
    }

    @Override
    public R get() {
      if (size() < maxSize) {
        return null;
      }
      nextResource %= size();
      R resource = get(nextResource++);
      return resource;
    }

    @Override
    public Collection<R> values() {
      return this;
    }

  }