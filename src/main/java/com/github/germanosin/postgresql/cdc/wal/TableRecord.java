package com.github.germanosin.postgresql.cdc.wal;

import java.util.Map;
import java.util.Optional;
import lombok.Data;

@Data
public class TableRecord implements Message {
  private final Table table;
  private final Operation operation;
  private final MessageType messageType;
  private final Map<String, TableColumn> columns;

  public enum Operation {
    INSERT,
    UPDATE,
    DELETE
  }

  public Optional<TableColumn> getColumn(final String columnName) {
    return Optional.ofNullable(
        this.columns.get(columnName)
    );
  }
}
