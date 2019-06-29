/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.realtime.impl.dictionary;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.writer.impl.DirectMemoryManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class OffHeapMutableBytesStoreTest {
  private static final int NUM_VALUES = 100_000;
  private static final int MAX_NUM_BYTES = 512;
  private static final Random RANDOM = new Random();

  private final PinotDataBufferMemoryManager _memoryManager = new DirectMemoryManager("testSegment");
  private final byte[][] _values = new byte[NUM_VALUES][];

  @BeforeClass
  public void setUp() {
    for (int i = 0; i < NUM_VALUES; i++) {
      int numBytes = RANDOM.nextInt(MAX_NUM_BYTES);
      byte[] value = new byte[numBytes];
      RANDOM.nextBytes(value);
      _values[i] = value;
    }
  }

  @Test
  public void testAdd()
      throws IOException {
    try (OffHeapMutableBytesStore offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(offHeapMutableBytesStore.add(_values[i]), i);
      }
    }
  }

  @Test
  public void testGet()
      throws IOException {
    try (OffHeapMutableBytesStore offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        offHeapMutableBytesStore.add(_values[i]);
      }
      for (int i = 0; i < NUM_VALUES; i++) {
        int index = RANDOM.nextInt(NUM_VALUES);
        assertTrue(Arrays.equals(offHeapMutableBytesStore.get(index), _values[index]));
      }
    }
  }

  @Test
  public void testEquals()
      throws IOException {
    try (OffHeapMutableBytesStore offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        offHeapMutableBytesStore.add(_values[i]);
      }
      for (int i = 0; i < NUM_VALUES; i++) {
        int index = RANDOM.nextInt(NUM_VALUES);
        assertTrue(offHeapMutableBytesStore.equals(index, _values[index]));
        if (!Arrays.equals(_values[index], _values[0])) {
          assertFalse(offHeapMutableBytesStore.equals(0, _values[index]));
          assertFalse(offHeapMutableBytesStore.equals(index, _values[0]));
        }
      }
    }
  }
}
