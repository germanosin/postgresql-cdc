package com.github.germanosin.postgresql.cdc.wal;

import lombok.Data;

@Data
public class TxMessage implements Message {
  private final MessageType messageType;
}
