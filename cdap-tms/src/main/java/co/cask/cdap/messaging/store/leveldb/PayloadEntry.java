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
import co.cask.cdap.messaging.store.PayloadTable;

/**
*
*/
public final class PayloadEntry implements PayloadTable.Entry {
  private final byte[] row;
  private final byte[] payload;

  public PayloadEntry(byte[] row, byte[] payload) {
    this.row = row;
    this.payload = payload;
  }

  @Override
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public long getTransactionWritePointer() {
    int offset = row.length - (2 * Long.BYTES) - Short.BYTES;
    return Bytes.toLong(row, offset, Long.BYTES);
  }

  @Override
  public long getPayloadWriteTimestamp() {
    int offset = row.length - Long.BYTES - Short.BYTES;
    return Bytes.toLong(row, offset, Long.BYTES);
  }

  @Override
  public short getPayloadSequenceId() {
    return Bytes.toShort(row, row.length - Short.BYTES);
  }
}
