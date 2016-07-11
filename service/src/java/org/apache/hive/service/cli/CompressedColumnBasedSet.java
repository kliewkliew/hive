package org.apache.hive.service.cli;

import org.apache.hive.service.cli.compression.CompDe;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.thrift.TException;

public class CompressedColumnBasedSet extends ColumnBasedSet {

  public CompressedColumnBasedSet(TRowSet tRowSet, CompDe compDe) throws TException {
      // Read from the stream using the protocol for each column in final schema
      super(compDe.decompress(tRowSet.getBinaryColumns()));
  }
}
