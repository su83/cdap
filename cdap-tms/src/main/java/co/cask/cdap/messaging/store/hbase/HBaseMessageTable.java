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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.PutBuilder;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.leveldb.MessageEntry;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * HBase implementation of {@link MessageTable}.
 */
public class HBaseMessageTable implements MessageTable {
  private static final byte[] PAYLOAD_COL = Bytes.toBytes('p');
  private static final byte[] TX_COL = Bytes.toBytes('t');
  private final Configuration hConf;
  private final HBaseTableUtil tableUtil;
  private final TableId tableId;
  private final byte[] columnFamily;
  private long writeTimestamp;
  private short seqId;
  private volatile HTable hTable;

  public HBaseMessageTable(Configuration hConf, HBaseTableUtil tableUtil, TableId tableId, byte[] columnFamily) {
    this.hConf = hConf;
    this.tableUtil = tableUtil;
    this.tableId = tableId;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long startTime, int limit, @Nullable Transaction transaction)
    throws IOException {
    byte[] startRow = Bytes.add(topicId.toBytes(), Bytes.toBytes(startTime));
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(Bytes.stopKeyForPrefix(topicId.toBytes()))
      .build();
    ResultScanner scanner = getHTable().getScanner(scan);
    return new HBaseCloseableIterator(scanner, limit, columnFamily, transaction);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, MessageId messageId, boolean inclusive,
                                        int limit, @Nullable Transaction transaction) throws IOException {
    byte[] startRow = Bytes.add(topicId.toBytes(), Bytes.toBytes(messageId.getPublishTimestamp()),
                                Bytes.toBytes(messageId.getSequenceId()));
    if (!inclusive) {
      startRow = Bytes.incrementBytes(startRow, 1);
    }
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(Bytes.stopKeyForPrefix(topicId.toBytes()))
      .build();
    ResultScanner scanner = getHTable().getScanner(scan);
    return new HBaseCloseableIterator(scanner, limit, columnFamily, transaction);
  }

  @Override
  public void store(TopicId topicId, Iterator<Entry> entries) throws IOException {
    long writeTs = System.currentTimeMillis();
    if (writeTs != writeTimestamp) {
      seqId = 0;
    }
    writeTimestamp = writeTs;
    while (entries.hasNext()) {
      Entry entry = entries.next();
      byte[] tableKey = Bytes.add(topicId.toBytes(), Bytes.toBytes(writeTimestamp), Bytes.toBytes(seqId++));
      PutBuilder putBuilder = tableUtil.buildPut(tableKey);
      if (entry.isPayloadReference()) {
        byte[] txPtrBytes = Bytes.toBytes(entry.getTransactionWritePointer());
        putBuilder.add(columnFamily, TX_COL, txPtrBytes);
      } else {
        putBuilder.add(columnFamily, PAYLOAD_COL, entry.getPayload());
        if (entry.isTransactional()) {
          putBuilder.add(columnFamily, TX_COL, Bytes.toBytes(entry.getTransactionWritePointer()));
        }
      }
      getHTable().put(putBuilder.build());
    }
  }

  @Override
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] rowPrefix = topicId.toBytes();
    byte[] stopRow = Bytes.stopKeyForPrefix(rowPrefix);
    Scan scan = tableUtil.buildScan()
      .setStartRow(rowPrefix)
      .setStopRow(stopRow)
      .build();

    try (ResultScanner scanner = getHTable().getScanner(scan)) {
      Iterator<Result> iterator = scanner.iterator();
      Result result = iterator.next();
      if (result.containsColumn(columnFamily, TX_COL)) {
        byte[] txCol = result.getValue(columnFamily, TX_COL);
        if (Bytes.equals(txCol, Bytes.toBytes(transactionWritePointer))) {
          Delete delete = tableUtil.buildDelete(result.getRow())
            .build();
          getHTable().delete(delete);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (hTable != null) {
      hTable.close();
    }
  }

  private HTable getHTable() throws IOException {
    if (hTable != null) {
      return hTable;
    }

    synchronized (this) {
      if (hTable != null) {
        return hTable;
      }
      hTable = tableUtil.createHTable(hConf, tableId);
      return hTable;
    }
  }

  private static class HBaseCloseableIterator extends AbstractCloseableIterator<Entry> {
    private final ResultScanner scanner;
    private final Transaction transaction;
    private final byte[] colFamily;
    private int limit;
    private boolean closed = false;

    HBaseCloseableIterator(ResultScanner scanner, int limit, byte[] colFamily, @Nullable Transaction transaction) {
      this.scanner = scanner;
      this.transaction = transaction;
      this.colFamily = colFamily;
      this.limit = limit;
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

      while (true) {
        Result result = null;
        try {
          result = scanner.next();
        } catch (IOException e) {
          Throwables.propagate(e);
        }

        if (result == null) {
          break;
        }
        MessageTable.Entry entry = new MessageEntry(result.getRow(), result.getValue(colFamily, PAYLOAD_COL),
                                                    result.getValue(colFamily, TX_COL));
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
