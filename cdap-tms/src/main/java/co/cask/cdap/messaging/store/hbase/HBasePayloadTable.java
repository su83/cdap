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
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.leveldb.PayloadEntry;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * HBase implementation of {@link PayloadTable}.
 */
public class HBasePayloadTable implements PayloadTable {
  private static final byte[] COL = Bytes.toBytes('c');
  private final Configuration hConf;
  private final HBaseTableUtil tableUtil;
  private final TableId tableId;
  private final byte[] columnFamily;
  private long writeTimestamp;
  private short pSeqId;
  private volatile HTable hTable;

  public HBasePayloadTable(Configuration hConf, HBaseTableUtil tableUtil, TableId tableId, byte[] columnFamily) {
    this.hConf = hConf;
    this.tableUtil = tableUtil;
    this.tableId = tableId;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long transactionWritePointer, MessageId messageId,
                                        boolean inclusive, int limit) throws IOException {
    byte[] startRow = Bytes.add(topicId.toBytes(), Bytes.toBytes(transactionWritePointer),
                                Bytes.toBytes(messageId.getWriteTimestamp()));
    startRow = Bytes.add(startRow, Bytes.toBytes(messageId.getPayloadSequenceId()));
    if (!inclusive) {
      startRow = Bytes.incrementBytes(startRow, 1);
    }
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(Bytes.stopKeyForPrefix(topicId.toBytes()))
      .setMaxResultSize(limit)
      .build();
    scan.setFilter(new FirstKeyOnlyFilter());
    final ResultScanner scanner = getHTable().getScanner(scan);

    return new AbstractCloseableIterator<Entry>() {
      private boolean closed = false;

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

        Result result = null;
        try {
          result = scanner.next();
        } catch (IOException e) {
          Throwables.propagate(e);
        }

        if (result != null) {
          return new PayloadEntry(result.getRow(), result.getValue(columnFamily, COL));
        }
        close();
        return null;
      }
    };
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
      Put put = tableUtil.buildPut(keyBytes)
        .add(columnFamily, COL, entry.getPayload())
        .build();
      getHTable().put(put);
    }
  }

  @Override
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] prefixRow = Bytes.add(topicId.toBytes(), Bytes.toBytes(transactionWritePointer));
    byte[] stopRow = Bytes.stopKeyForPrefix(prefixRow);
    Scan scan = tableUtil.buildScan()
      .setStartRow(prefixRow)
      .setStopRow(stopRow)
      .build();
    try (ResultScanner scanner = getHTable().getScanner(scan)) {
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        Result result = iterator.next();
        Delete delete = tableUtil.buildDelete(result.getRow())
          .build();
        getHTable().delete(delete);
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
}
