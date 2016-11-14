/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.messaging.store.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.messaging.store.MessageTable;

import javax.annotation.Nullable;

/**
* Message Entry.
*/
public class MessageEntry implements MessageTable.Entry {
  private final byte[] row;
  private final byte[] payload;
  private final byte[] txPtr;

  public MessageEntry(byte[] row, @Nullable byte[] payload, @Nullable byte[] txPtr) {
    this.row = row;
    this.payload = payload;
    this.txPtr = txPtr;
  }

  @Override
  public boolean isPayloadReference() {
    return payload == null;
  }

  @Override
  public boolean isTransactional() {
    return txPtr != null;
  }

  @Override
  public long getTransactionWritePointer() {
    if (txPtr != null) {
      return Bytes.toLong(txPtr);
    }
    return 0;
  }

  @Nullable
  @Override
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public long getPublishTimestamp() {
    return Bytes.toLong(row, row.length - Short.BYTES - Long.BYTES);
  }

  @Override
  public short getSequenceId() {
    return Bytes.toShort(row, row.length - Short.BYTES);
  }
}
