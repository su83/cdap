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

package co.cask.cdap.messaging.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for Payload Table tests.
 */
public abstract class PayloadTableTest {

  @Test
  public void testStore() throws Exception {
    try (PayloadTable table = getTable()) {
      TopicId id = NamespaceId.DEFAULT.topic("t1");
      List<PayloadTable.Entry> entryList = new ArrayList<>();
      final byte[] payloadBytes = Bytes.toBytes("data");
      final long writePtr = 100L;
      entryList.add(new PayloadTable.Entry() {
        @Override
        public byte[] getPayload() {
          return payloadBytes;
        }

        @Override
        public long getTransactionWritePointer() {
          return writePtr;
        }

        @Override
        public long getPayloadWriteTimestamp() {
          return 0;
        }

        @Override
        public short getPayloadSequenceId() {
          return 0;
        }
      });
      table.store(id, entryList.iterator());
      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);
      CloseableIterator<PayloadTable.Entry> iterator = table.fetch(id, writePtr, new MessageId(messageId), true, 50);
      PayloadTable.Entry entry = iterator.next();
      Assert.assertArrayEquals(payloadBytes, entry.getPayload());
      Assert.assertEquals(writePtr, entry.getTransactionWritePointer());
      Assert.assertFalse(iterator.hasNext());
      table.delete(id, writePtr);
      iterator = table.fetch(id, writePtr, new MessageId(messageId), true, 50);
      Assert.assertFalse(iterator.hasNext());
    }
  }

  protected abstract PayloadTable getTable() throws Exception;
}
