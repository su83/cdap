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
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.proto.id.TopicId;
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * LevelDB implementation of {@link MessageTable}.
 */
public class LevelDBMessageTable implements MessageTable {
  private static final byte[] PAYLOAD_COL = Bytes.toBytes('p');
  private static final byte[] TX_COL = Bytes.toBytes('t');
  private final LevelDBTableCore core;
  private long writeTimestamp;
  private short pSeqId;

  public LevelDBMessageTable(LevelDBTableService service, String tableName) throws IOException {
    this.core = new LevelDBTableCore(tableName, service);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long startTime, int limit, @Nullable Transaction transaction)
    throws IOException {
    byte[] startRow = Bytes.add(topicId.toBytes(), Bytes.toBytes(startTime));
    byte[] stopKey = Bytes.stopKeyForPrefix(topicId.toBytes());
    final Scanner scanner = core.scan(startRow, stopKey, null, null, null);
    return new LevelDBCloseableIterator(scanner, limit, transaction);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, MessageId messageId, boolean inclusive, final int limit,
                                        @Nullable Transaction transaction) throws IOException {
    byte[] startRow = Bytes.add(topicId.toBytes(), Bytes.toBytes(messageId.getPublishTimestamp()),
                                Bytes.toBytes(messageId.getSequenceId()));
    if (!inclusive) {
      startRow = Bytes.incrementBytes(startRow, 1);
    }
    byte[] stopKey = Bytes.stopKeyForPrefix(topicId.toBytes());
    final Scanner scanner = core.scan(startRow, stopKey, null, null, null);
    return new LevelDBCloseableIterator(scanner, limit, transaction);
  }

  @Override
  public void store(TopicId topicId, Iterator<Entry> entries) throws IOException {
    long writeTs = System.currentTimeMillis();
    if (writeTs != writeTimestamp) {
      pSeqId = 0;
    }
    writeTimestamp = writeTs;
    byte[] tableKey = Bytes.add(topicId.toBytes(), Bytes.toBytes(writeTimestamp), Bytes.toBytes(pSeqId++));
    while (entries.hasNext()) {
      Entry entry = entries.next();
      if (entry.isPayloadReference()) {
        byte[] txPtrBytes = Bytes.toBytes(entry.getTransactionWritePointer());
        core.put(tableKey, TX_COL, txPtrBytes, -1);
      } else {
        core.put(tableKey, PAYLOAD_COL, entry.getPayload(), -1);
        if (entry.isTransactional()) {
          core.put(tableKey, TX_COL, Bytes.toBytes(entry.getTransactionWritePointer()), -1);
        }
      }
    }
  }

  @Override
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] rowPrefix = topicId.toBytes();
    byte[] stopRow = Bytes.stopKeyForPrefix(rowPrefix);
    Scanner scan = core.scan(rowPrefix, stopRow, null, null, null);
    while (true) {
      Row row = scan.next();
      if (row == null) {
        break;
      }

      Map<byte[], byte[]> cols = core.getRow(row.getRow(), null, null, null, -1, null);
      byte[] txPtr = cols.get(TX_COL);
      if (Bytes.equals(txPtr, Bytes.toBytes(transactionWritePointer))) {
        core.deleteRows(row.getRow());
      }
    }
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  private static class LevelDBCloseableIterator extends AbstractCloseableIterator<Entry> {
    private final Scanner scanner;
    private final Transaction transaction;
    private int limit;
    private boolean closed = false;

    LevelDBCloseableIterator(Scanner scanner, int limit, @Nullable Transaction transaction) {
      this.scanner = scanner;
      this.limit = limit;
      this.transaction = transaction;
    }

    @Override
    protected MessageTable.Entry computeNext() {
      if (closed) {
        return endOfData();
      }

      if (limit <= 0) {
        close();
        return null;
      }

      while (true) {
        Row row = scanner.next();
        if (row == null) {
          break;
        }
        MessageTable.Entry entry = new MessageEntry(row.getRow(), row.get(PAYLOAD_COL), row.get(TX_COL));
        if (validEntry(entry, transaction)) {
          limit--;
          return entry;
        }
      }
      close();
      return null;
    }

    @Override
    public void close() {
      scanner.close();
      endOfData();
      closed = true;
    }

    private static boolean validEntry(MessageTable.Entry entry, Transaction transaction) {
      if (transaction == null || (!entry.isTransactional())) {
        return true;
      }

      long txWritePtr = entry.getTransactionWritePointer();
      return transaction.isVisible(txWritePtr);
    }
  }
}
