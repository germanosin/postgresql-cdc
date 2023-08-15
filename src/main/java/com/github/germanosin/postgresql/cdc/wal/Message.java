package com.github.germanosin.postgresql.cdc.wal;

import java.util.List;

public interface Message {
  MessageType getMessageType();

  default boolean is(MessageType type) {
    return getMessageType().equals(type);
  }

  default boolean any(List<MessageType> types) {
    return types.stream().anyMatch(s -> s.equals(getMessageType()));
  }
}
