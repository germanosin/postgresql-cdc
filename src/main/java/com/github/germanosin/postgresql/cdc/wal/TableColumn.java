package com.github.germanosin.postgresql.cdc.wal;

import com.github.germanosin.postgresql.cdc.PgArrayUtil;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;
import lombok.Data;

@Data
public class TableColumn {
  private final String name;
  private final String type;
  private final String value;

  public Long asInt64() {
    return notNull(Long::valueOf);
  }

  public Integer asInt32() {
    return notNull(Integer::valueOf);
  }

  public String asString() {
    return value;
  }

  public List<String> asStringArray() {
    return notNull(PgArrayUtil::parseStringArray);
  }

  public boolean asBoolean() {
    return "t".equalsIgnoreCase(value);
  }

  public BigDecimal asBigDecimal() {
    return notNull(BigDecimal::new);
  }

  public List<Long> asLongArray() {
    return notNull(PgArrayUtil::parseLongArray);
  }

  public List<Integer> asIntegerArray() {
    return notNull(PgArrayUtil::parseIntArray);
  }

  public List<Double> asDoubleArray() {
    return notNull(PgArrayUtil::parseDoubleArray);
  }

  public boolean isNull() {
    return value == null;
  }

  private  <T> T notNull(Function<String, T> function) {
    return value != null ? function.apply(value) : null;
  }
}
