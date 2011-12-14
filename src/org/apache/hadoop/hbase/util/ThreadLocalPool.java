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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.PoolMap.Pool;

/**
 * The <code>ThreadLocalPool</code> represents a {@link PoolMap.Pool} that
 * builds on the {@link ThreadLocal} class. It essentially binds the resource to
 * the thread from which it is accessed.
 * 
 * <p>
 * If {@link #maxSize} is set to {@link Integer#MAX_VALUE}, then the size of the
 * pool is bounded only by the number of threads that add resources to this
 * pool. Otherwise, it caps the number of threads that can set a value on this
 * {@link ThreadLocal} instance to the (non-zero positive) value specified in
 * {@link #maxSize}.
 * </p>
 * 
 * 
 * @author Karthick Sankarachary
 * 
 * @param <R>
 *          the type of the resource
 */
class ThreadLocalPool<R> extends ThreadLocal<R> implements Pool<R> {
  public static Map<ThreadLocalPool<?>, List<Object>> poolResources = new HashMap<ThreadLocalPool<?>, List<Object>>();

  private int maxSize;

  public ThreadLocalPool(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public synchronized R put(R resource) {
    R previousResource = get();
    List<Object> resources = getResources(this);
    if (previousResource != null) {
      resources.remove(previousResource);
    }
    if (resources.size() >= maxSize) {
      return resource;
    }
    this.set(resource);
    if (resource != null && resource != previousResource) {
      resources.add(resource);
    }
    return previousResource;
  }

  @Override
  public void remove() {
    R resource = super.get();
    super.remove();
    if (resource != null) {
      getResources(this).remove(resource);
    }
  }

  @Override
  public int size() {
    return getResources(this).size();
  }

  @Override
  public boolean remove(R resource) {
    if (resource == null) {
      return false;
    }
    if (resource.equals(get())) {
      remove();
      return true;
    }
    return false;
  }

  @Override
  public void clear() {
    poolResources.clear();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<R> values() {
    List<R> resources = Collections.synchronizedList(new LinkedList<R>());
    for (Object resource : getResources(this)) {
      if (resource != null) {
	resources.add((R) resource);
      }
    }
    return resources;
  }

  private synchronized static List<Object> getResources(ThreadLocalPool<?> pool) {
    List<Object> resources = poolResources.get(pool);
    if (resources == null) {
      resources = new LinkedList<Object>();
      poolResources.put(pool, resources);
    }
    return resources;
  }
}