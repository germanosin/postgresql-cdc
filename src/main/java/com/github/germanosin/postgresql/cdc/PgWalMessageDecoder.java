package com.github.germanosin.postgresql.cdc;

import static com.github.germanosin.postgresql.cdc.PgWalMessageDecoder.TupleDataSubMessageType.NULL;
import static com.github.germanosin.postgresql.cdc.PgWalMessageDecoder.TupleDataSubMessageType.TEXT;
import static com.github.germanosin.postgresql.cdc.PgWalMessageDecoder.TupleDataSubMessageType.UNCHANGED;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import com.github.germanosin.postgresql.cdc.wal.ColumnMeta;
import com.github.germanosin.postgresql.cdc.wal.Message;
import com.github.germanosin.postgresql.cdc.wal.MessageType;
import com.github.germanosin.postgresql.cdc.wal.Table;
import com.github.germanosin.postgresql.cdc.wal.TableColumn;
import com.github.germanosin.postgresql.cdc.wal.TableRecord;
import com.github.germanosin.postgresql.cdc.wal.TxMessage;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class PgWalMessageDecoder {
  private static final int COLUMN_NAME_ID = 4;
  private static final int COLUMN_OID = 5;
  private static final int COLUMN_TYPE_ID = 6;

  // Value list should be an ordered one to preserve column order coming from RELATION messages
  private final Map<Integer, Table> tables = new HashMap<>();

  // Returns PgWALMessage in case of INSERT and UPDATE messages, otherwise returns empty Optional
  public Optional<Message> decode(final ByteBuffer buffer, PgConnectionFactory connectionFactory) {
    final MessageType messageType = MessageType.forType((char) buffer.get());

    log.debug("Received message type {}", messageType);

    switch (messageType) {
      case RELATION:
        handleRelationMessage(buffer, connectionFactory);
        return Optional.empty();
      case INSERT:
        return Optional.of(decodeInsertMessage(buffer));
      case UPDATE:
        return Optional.of(decodeUpdateMessage(buffer));
      case DELETE:
        return Optional.of(decodeDeleteMessage(buffer));
      case BEGIN:
      case COMMIT:
        return Optional.of(new TxMessage(messageType));
      default:
        return Optional.empty();
    }
  }

  private void handleRelationMessage(final ByteBuffer buffer,
                                     PgConnectionFactory connectionFactory) {
    try (final Connection metadataConnection = connectionFactory.getConnection()) {
      final int relationId = buffer.getInt();
      final String schemaName = readString(buffer);
      final String tableName = readString(buffer);
      // skipping replica identity id for redundancy
      buffer.get();
      final short columnCount = buffer.getShort();

      log.debug("Event: {}, RelationId: {}, Columns: {}",
          MessageType.RELATION, relationId, columnCount
      );
      log.debug("Schema: '{}', Table: '{}'", schemaName, tableName);

      final DatabaseMetaData databaseMetadata = metadataConnection.getMetaData();
      final List<ColumnMeta> columns = new LinkedList<>();

      try (final ResultSet rs = databaseMetadata.getColumns(
          null, schemaName, tableName, null)
      ) {
        while (rs.next()) {
          final String name = rs.getString(COLUMN_NAME_ID);
          final int oid = rs.getInt(COLUMN_OID);
          final String type = rs.getString(COLUMN_TYPE_ID);

          columns.add(new ColumnMeta(name, type, oid));
        }
      }
      tables.put(relationId, new Table(schemaName, tableName, relationId, columns));
    } catch (final Exception e) {
      log.error("Error occurred while handling RELATION message: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private TableRecord decodeInsertMessage(final ByteBuffer buffer) {
    final int relationId = buffer.getInt();

    // Skipping tuple type char.
    // Must be "N" for inserts
    buffer.get();

    final Table table = tables.get(relationId);
    if (table == null) {
      throw new RuntimeException("No column meta for relation ID " + relationId);
    }

    return readTupleDataForColumns(buffer, table.getColumns())
        .stream()
        .collect(collectingAndThen(
            toMap(TableColumn::getName, identity()),
            columns -> new TableRecord(
                table, TableRecord.Operation.INSERT, MessageType.INSERT, columns
            )
        ));
  }

  private TableRecord decodeDeleteMessage(ByteBuffer buffer) {
    final int relationId = buffer.getInt();

    final char tupleType = (char) buffer.get();

    final Table table = tables.get(relationId);

    if (table == null) {
      throw new RuntimeException("No column meta for relation ID " + relationId);
    }

    return readTupleDataForColumns(buffer, table.getColumns())
        .stream()
        .collect(collectingAndThen(
            toMap(TableColumn::getName, identity()),
            columns -> new TableRecord(
                table,
                TableRecord.Operation.DELETE,
                MessageType.DELETE,
                columns
            )
        ));
  }


  private TableRecord decodeUpdateMessage(final ByteBuffer buffer) {
    final int relationId = buffer.getInt();

    final char tupleType = (char) buffer.get();

    final Table table = tables.get(relationId);

    if (table == null) {
      throw new RuntimeException("No column meta for relation ID %" + relationId);
    }

    // K = Identifies the following TupleData sub-message as a key
    // O = Identifies the following TupleData sub-message as an old tuple
    // Skipping as we don't need old tuple data at the moment
    if ('O' == tupleType || 'K' == tupleType) {
      skipColumnTupleData(buffer);

      // Skipping the 'N' tuple type
      buffer.get();
    }

    return readTupleDataForColumns(buffer, table.getColumns())
        .stream()
        .collect(collectingAndThen(
            toMap(TableColumn::getName, identity()),
            columns -> new TableRecord(
                table,
                TableRecord.Operation.UPDATE,
                MessageType.UPDATE,
                columns
            )
        ));
  }

  private List<TableColumn> readTupleDataForColumns(
      final ByteBuffer buffer, final List<ColumnMeta> columnMetaList) {
    final List<TableColumn> columns = new ArrayList<>();

    final short numberOfColumns = buffer.getShort();

    for (short i = 0; i < numberOfColumns; ++i) {
      final TupleDataSubMessageType tupleDataSubMessageType =
          TupleDataSubMessageType.forType((char) buffer.get());

      final ColumnMeta columnMeta = columnMetaList.get(i);

      if (tupleDataSubMessageType.equals(TEXT)) {
        columns.add(
            new TableColumn(
                columnMeta.getName(),
                columnMeta.getType(),
                readColumnValueAsString(buffer)
            )
        );
      } else if (tupleDataSubMessageType.equals(NULL)) {
        columns.add(
            new TableColumn(columnMeta.getName(), columnMeta.getType(), null)
        );
      } else if (tupleDataSubMessageType.equals(UNCHANGED)) {
        log.warn("Column: {}, Value: UNCHANGED", columnMeta.getName());
      } else {
        throw new IllegalArgumentException(
            "Unknown tuple data sub message type: " + tupleDataSubMessageType);
      }
    }

    return columns;
  }

  private void skipColumnTupleData(final ByteBuffer buffer) {
    final short numberOfColumns = buffer.getShort();

    for (short i = 0; i < numberOfColumns; ++i) {
      final TupleDataSubMessageType tupleDataSubMessageType =
          TupleDataSubMessageType.forType((char) buffer.get());

      if (tupleDataSubMessageType == TEXT) {
        readColumnValueAsString(buffer);
      }
    }
  }



  public enum TupleDataSubMessageType {
    TEXT,
    UNCHANGED,
    NULL;

    private static final Map<Character, TupleDataSubMessageType> types = Map.of(
        't', TEXT,
        'u', UNCHANGED,
        'n', NULL
    );

    public static TupleDataSubMessageType forType(final char type) {
      TupleDataSubMessageType result = types.get(type);
      if (result == null) {
        throw new IllegalArgumentException("Unsupported sub-message type: " + type);
      }
      return result;
    }
  }

  private static String readString(final ByteBuffer buffer) {
    final StringBuilder sb = new StringBuilder();
    byte b;
    while ((b = buffer.get()) != 0) {
      sb.append((char) b);
    }
    return sb.toString();
  }

  private static String readColumnValueAsString(final ByteBuffer buffer) {
    final int length = buffer.getInt();
    final byte[] value = new byte[length];
    buffer.get(value, 0, length);
    return new String(value, StandardCharsets.UTF_8);
  }
}