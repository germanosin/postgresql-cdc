package com.github.germanosin.postgresql.cdc.wal;

import java.util.List;
import lombok.Data;

@Data
public class Table {
  private final String schema;
  private final String name;
  private final int relationId;
  private final List<ColumnMeta> columns;
}
