package com.cloudera.joshelser;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class ReadSnapshotFile {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: <snapshot-name> <restore-dir>");
      System.exit(1);
    }
    Configuration conf = HBaseConfiguration.create();
    JobConf jc = new JobConf(conf);
    TableSnapshotInputFormat.setInput(jc, args[0], new Path(args[1]));
    //setScanInfo(jc);
    setStreamScanInfo(jc);

    TableSnapshotInputFormat inputFormat = new TableSnapshotInputFormat();
    InputSplit[] splits = inputFormat.getSplits(jc, 1);
    if (splits.length != 1) {
      throw new RuntimeException("Expected one split, saw " + splits.length);
    }

    RecordReader<ImmutableBytesWritable,Result> rr = inputFormat.getRecordReader(splits[0], jc, null); 
    ImmutableBytesWritable key = (ImmutableBytesWritable) rr.createKey();
    Result value = (Result) rr.createValue();

    long numRows = 0l;
    byte[] lastRow = null;
    long uniqueRows = 0l;
    while (rr.next(key, value)) {
      if (lastRow != null) {
        if (0 != key.compareTo(lastRow)) {
          uniqueRows++;
        }
      } else {
        uniqueRows++;
      }
      lastRow = key.copyBytes();
      numRows++;
    }
    System.out.println("Total rows " + numRows);
    System.out.println("Unique rows " + uniqueRows);

    rr.close();
  }

  private static void setScanInfo(JobConf jc) {
    jc.set(TableInputFormat.COLUMN_LIST, "info0");
  }

  private static void setStreamScanInfo(JobConf jc) throws IOException {
    Scan s = new Scan();
    s.setReadType(ReadType.STREAM);
    s.addFamily(Bytes.toBytes("info0"));
    String serializedScan = org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertScanToString(s);
    jc.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, serializedScan);
  }
}
