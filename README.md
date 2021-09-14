# Load data

```
$ HBASE_OPTS="-Xmx4G" hbase pe --rows=20000000 --nomapred sequentialWrite 4
```

Merge the regions back together if they get split, and use `major_compact` to make sure you
have a single HFile. Make sure you have an HFile which is at least 5G in size.

Then, create a snapshot
```
hbase> snapshot 'TestTable', 'josh1'
```

# Build the jar

```
$ mvn package
```


# Run with HBase 2 jars

Run simply via:

```
$ time java -cp stream-repro-0.0.1-SNAPSHOT.jar:$(hbase classpath):$(hbase mapredcp) com.cloudera.joshelser.ReadSnapshotFile josh1 /tmp/josh1-snap
```

The second argument is simply a unique directory which you have access to write to in HDFS.
