package com.github.germanosin.postgresql.cdc;

import com.github.germanosin.postgresql.cdc.wal.TableRecord;
import java.util.ArrayList;
import java.util.List;

public class DbChangeConsumer implements CdcConsumer {
  private volatile boolean started;
  private final List<TableRecord> handled = new ArrayList<>();

  @Override
  public boolean isStarted() {
    return started;
  }

  @Override
  public void markStarted() {
    this.started = true;
  }

  public List<TableRecord> getHandled() {
    return handled;
  }

  public void clear() {
    this.handled.clear();
  }

  @Override
  public void handleBatch(List<TableRecord> records) {
    this.handled.addAll(records);
  }
}
