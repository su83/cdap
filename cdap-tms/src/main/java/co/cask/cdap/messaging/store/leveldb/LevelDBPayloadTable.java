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
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.proto.id.TopicId;

import java.io.IOException;
import java.util.Iterator;

/**
 * LevelDB implementation of {@link PayloadTable}.
 */
public class LevelDBPayloadTable implements PayloadTable {
  private static final byte[] COL = Bytes.toBytes('c');
  private final LevelDBTableCore core;
  private long writeTimestamp;
  private short pSeqId;

  public LevelDBPayloadTable(LevelDBTableService service, String tableName) throws IOException {
    this.core = new LevelDBTableCore(tableName, service);
    this.pSeqId = 0;
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long transactionWriterPointer, MessageId messageId,
                                        boolean inclusive, final int limit)
    throws IOException {
    byte[] startKey = Bytes.add(topicId.toBytes(), Bytes.toBytes(transactionWriterPointer),
                                Bytes.toBytes(messageId.getWriteTimestamp()));
    startKey = Bytes.add(startKey, Bytes.toBytes(messageId.getPayloadSequenceId()));
    if (!inclusive) {
      startKey = Bytes.incrementBytes(startKey, 1);
    }
    byte[] stopKey = Bytes.stopKeyForPrefix(topicId.toBytes());
    final Scanner scanner = core.scan(startKey, stopKey, null, null, null);
    return new LevelDBCloseableIterator(scanner, limit);
  }

  @Override
  public void store(TopicId topicId, Iterator<Entry> entries) throws IOException {
    long writeTs = System.currentTimeMillis();
    if (writeTs != writeTimestamp) {
      pSeqId = 0;
    }
    writeTimestamp = writeTs;
    while (entries.hasNext()) {
      Entry entry = entries.next();
      byte[] tableKey = Bytes.add(topicId.toBytes(), Bytes.toBytes(entry.getTransactionWritePointer()));
      byte[] keyBytes = Bytes.add(tableKey, Bytes.toBytes(writeTimestamp), Bytes.toBytes(pSeqId++));
      core.put(keyBytes, COL, entry.getPayload(), -1);
    }
  }

  @Override
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] startRow = Bytes.add(topicId.toBytes(), Bytes.toBytes(transactionWritePointer));
    core.deleteRows(startRow);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  private static class LevelDBCloseableIterator extends AbstractCloseableIterator<Entry> {
    private final Scanner scanner;
    private int limit;
    private boolean closed = false;

    LevelDBCloseableIterator(Scanner scanner, int limit) {
      this.scanner = scanner;
      this.limit = limit;
    }

    @Override
    public void close() {
      scanner.close();
      endOfData();
      closed = true;
    }

    @Override
    protected Entry computeNext() {
      if (closed) {
        return endOfData();
      }

      if (limit <= 0) {
        close();
        return null;
      }

      Row row = scanner.next();
      if (row != null) {
        limit--;
        return new PayloadEntry(row.getRow(), row.get(COL));
      }
      close();
      return null;
    }
  }
}
