package com.github.germanosin.postgresql.cdc;

import com.github.germanosin.postgresql.cdc.wal.TableRecord;
import java.util.List;

public interface CdcConsumer {
  boolean isStarted();

  void markStarted();

  void handleBatch(List<TableRecord> records);
}
