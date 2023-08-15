package com.github.germanosin.postgresql.cdc.wal;

import lombok.Data;

@Data
public class ColumnMeta {
  private final String name;
  private final String type;
  private final int oid;
}
