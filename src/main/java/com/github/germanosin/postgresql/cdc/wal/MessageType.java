package com.github.germanosin.postgresql.cdc.wal;

import java.util.Map;

public enum MessageType {
  RELATION,
  BEGIN,
  COMMIT,
  INSERT,
  UPDATE,
  DELETE,
  TYPE,
  ORIGIN,
  TRUNCATE,
  LOGICAL_DECODING_MESSAGE;

  static final Map<Character, MessageType> chars = Map.of(
      'R', RELATION,
      'B', BEGIN,
      'C', COMMIT,
      'I', INSERT,
      'U', UPDATE,
      'D', DELETE,
      'Y', TYPE,
      'O', ORIGIN,
      'T', TRUNCATE,
      'M', LOGICAL_DECODING_MESSAGE
  );

  public static MessageType forType(final char type) {
    MessageType messageType = chars.get(type);
    if (messageType == null) {
      throw new IllegalArgumentException("Unsupported message type: " + type);
    }
    return messageType;
  }
}
