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

import java.io.Closeable;
import java.io.IOException;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * Off-heap variable length mutable bytes store.
 * <p>The class is thread-safe for single writer and multiple readers.
 * <p>There are two sets of buffers allocated for the store, one set to store the value end offsets and the other set to
 * store the values. The new buffer is allocated when the previous buffer is filled up. The first entry of each offset
 * buffer stores the end offset of the last value (or 0 for the first offset buffer) so that we can always get the
 * previous and current value's end offset from the same buffer.
 */
public class OffHeapMutableBytesStore implements Closeable {
  private static final byte[] EMPTY_BYTES = new byte[0];

  // MAX_NUM_BUFFERS = 8_192
  private static final int MAX_NUM_BUFFERS = 1 << 13;

  // OFFSET_BUFFER_SIZE = 32_768 + 4
  private static final int OFFSET_BUFFER_SHIFT_OFFSET = 13;
  private static final int OFFSET_BUFFER_SIZE = ((1 << OFFSET_BUFFER_SHIFT_OFFSET) + 1) << 2;
  private static final int OFFSET_BUFFER_MASK = 0xFFFFFFFF >>> (Integer.SIZE - OFFSET_BUFFER_SHIFT_OFFSET);

  // VALUE_BUFFER_SIZE = 1_048_576
  private static final int VALUE_BUFFER_SIFT_OFFSET = 20;
  private static final int VALUE_BUFFER_SIZE = 1 << VALUE_BUFFER_SIFT_OFFSET;
  private static final int VALUE_BUFFER_MASK = 0xFFFFFFFF >>> (Integer.SIZE - VALUE_BUFFER_SIFT_OFFSET);

  private final PinotDataBuffer[] _offsetBuffers = new PinotDataBuffer[MAX_NUM_BUFFERS];
  private final PinotDataBuffer[] _valueBuffers = new PinotDataBuffer[MAX_NUM_BUFFERS];
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;

  private int _numValues;
  private int _valueStartOffset;
  private int _totalBufferSize;

  public OffHeapMutableBytesStore(PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
  }

  public int add(byte[] value) {
    int offsetBufferIndex = _numValues >>> OFFSET_BUFFER_SHIFT_OFFSET;
    int offsetIndex = _numValues & OFFSET_BUFFER_MASK;

    PinotDataBuffer offsetBuffer;
    // If this is the first entry in the offset buffer, allocate a new buffer and store the start offset (end offset of
    // the previous value)
    if (offsetIndex == 0) {
      offsetBuffer = _memoryManager.allocate(OFFSET_BUFFER_SIZE, _allocationContext);
      offsetBuffer.putInt(0, _valueStartOffset);
      _offsetBuffers[offsetBufferIndex] = offsetBuffer;
      _totalBufferSize += OFFSET_BUFFER_SIZE;
    } else {
      offsetBuffer = _offsetBuffers[offsetBufferIndex];
    }

    int valueLength = value.length;
    if (valueLength == 0) {
      offsetBuffer.putInt((offsetIndex + 1) << 2, _valueStartOffset);
      return _numValues++;
    }

    int valueBufferIndex = (_valueStartOffset + valueLength - 1) >>> VALUE_BUFFER_SIFT_OFFSET;
    PinotDataBuffer valueBuffer;
    // If the current value buffer does not have enough space, allocate a new buffer to store the value
    if ((_valueStartOffset - 1) >>> VALUE_BUFFER_SIFT_OFFSET != valueBufferIndex) {
      valueBuffer = _memoryManager.allocate(VALUE_BUFFER_SIZE, _allocationContext);
      _valueBuffers[valueBufferIndex] = valueBuffer;
      _totalBufferSize += VALUE_BUFFER_SIZE;
      _valueStartOffset = valueBufferIndex << VALUE_BUFFER_SIFT_OFFSET;
    } else {
      valueBuffer = _valueBuffers[valueBufferIndex];
    }

    int valueEndOffset = _valueStartOffset + valueLength;
    offsetBuffer.putInt((offsetIndex + 1) << 2, valueEndOffset);
    valueBuffer.readFrom(_valueStartOffset & VALUE_BUFFER_MASK, value);
    _valueStartOffset = valueEndOffset;

    return _numValues++;
  }

  public byte[] get(int index) {
    int offsetBufferIndex = index >>> OFFSET_BUFFER_SHIFT_OFFSET;
    int offsetIndex = index & OFFSET_BUFFER_MASK;
    int previousValueEndOffset = _offsetBuffers[offsetBufferIndex].getInt(offsetIndex << 2);
    int valueEndOffset = _offsetBuffers[offsetBufferIndex].getInt((offsetIndex + 1) << 2);

    if (previousValueEndOffset == valueEndOffset) {
      return EMPTY_BYTES;
    }

    int valueBufferIndex = (valueEndOffset - 1) >>> VALUE_BUFFER_SIFT_OFFSET;
    int startOffsetInValueBuffer;
    int valueLength;
    if ((previousValueEndOffset - 1) >>> VALUE_BUFFER_SIFT_OFFSET != valueBufferIndex) {
      // The first value in the value buffer
      startOffsetInValueBuffer = 0;
      valueLength = valueEndOffset & VALUE_BUFFER_MASK;
    } else {
      // Not the first value in the value buffer
      startOffsetInValueBuffer = previousValueEndOffset & VALUE_BUFFER_MASK;
      valueLength = valueEndOffset - previousValueEndOffset;
    }

    byte[] value = new byte[valueLength];
    _valueBuffers[valueBufferIndex].copyTo(startOffsetInValueBuffer, value);
    return value;
  }

  public boolean equals(int index, byte[] value) {
    int valueLength = value.length;

    int offsetBufferIndex = index >>> OFFSET_BUFFER_SHIFT_OFFSET;
    int offsetIndex = index & OFFSET_BUFFER_MASK;
    int previousValueEndOffset = _offsetBuffers[offsetBufferIndex].getInt(offsetIndex << 2);
    int valueEndOffset = _offsetBuffers[offsetBufferIndex].getInt((offsetIndex + 1) << 2);

    if (valueLength == 0) {
      return previousValueEndOffset == valueEndOffset;
    }

    int valueBufferIndex = (valueEndOffset - 1) >>> VALUE_BUFFER_SIFT_OFFSET;
    int startOffsetInValueBuffer;
    if ((previousValueEndOffset - 1) >>> VALUE_BUFFER_SIFT_OFFSET != valueBufferIndex) {
      // The first value in the value buffer
      if ((valueEndOffset & VALUE_BUFFER_MASK) != valueLength) {
        return false;
      }
      startOffsetInValueBuffer = 0;
    } else {
      // Not the first value in the value buffer
      if (valueEndOffset - previousValueEndOffset != valueLength) {
        return false;
      }
      startOffsetInValueBuffer = previousValueEndOffset & VALUE_BUFFER_MASK;
    }

    byte[] valueAtIndex = new byte[valueLength];
    _valueBuffers[valueBufferIndex].copyTo(startOffsetInValueBuffer, valueAtIndex);
    for (int i = 0; i < valueLength; i++) {
      if (value[i] != valueAtIndex[i]) {
        return false;
      }
    }
    return true;
  }

  public int getTotalBufferSize() {
    return _totalBufferSize;
  }

  @Override
  public void close()
      throws IOException {
    for (PinotDataBuffer offsetBuffer : _offsetBuffers) {
      if (offsetBuffer != null) {
        offsetBuffer.close();
      } else {
        break;
      }
    }
    for (PinotDataBuffer valueBuffer : _valueBuffers) {
      if (valueBuffer != null) {
        valueBuffer.close();
      } else {
        break;
      }
    }
  }
}
