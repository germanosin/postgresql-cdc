package com.github.germanosin.postgresql.cdc;

import com.github.germanosin.postgresql.cdc.wal.MessageType;
import com.github.germanosin.postgresql.cdc.wal.TableColumn;
import com.github.germanosin.postgresql.cdc.wal.TableRecord;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class IntegrationTest {
  @ClassRule
  public static final PostgreSQLContainer<?> postgresqlContainer =
      new PostgreSQLContainer<>(DockerImageName.parse("debezium/postgres:12")
          .asCompatibleSubstituteFor("postgres"));

  private static final String createTable = "CREATE TABLE test (\n" +
      "    \"id\" bigserial, \n" +
      "  %s  " +
      "    PRIMARY KEY (id)\n" +
      ")";

  private static class Field<T> {
    String type;
    Supplier<T> generator;
    Function<TableColumn, T> getter;
    Function<T, String> serializer;

    String generateString() {
      return serializer.apply(generator.get());
    }

    String getString(TableColumn column) {
      return serializer.apply(getter.apply(column));
    }

    public Field(String type, Supplier<T> generator, Function<TableColumn, T> getter,
                 Function<T, String> serializer) {
      this.type = type;
      this.generator = generator;
      this.getter = getter;
      this.serializer = serializer;
    }
  }

  private static final Map<String, Field<?>> fields = Map.of(
      "string", new Field<>("character varying",
          () -> UUID.randomUUID().toString(),
          TableColumn::asString, Object::toString
      ),
      "int", new Field<>("int",
          () -> ThreadLocalRandom.current().nextInt(0, 1000000),
          TableColumn::asInt32, Object::toString
      ),
      "bigint", new Field<>("bigint",
          () -> ThreadLocalRandom.current().nextLong(0, 1000000),
          TableColumn::asInt64, Object::toString
      ),
      "bool", new Field<>( "boolean",
          () -> ThreadLocalRandom.current().nextBoolean(),
          TableColumn::asBoolean, Object::toString
      ),
      "int_array", new Field<>( "int[]",
          () -> List.of(
              ThreadLocalRandom.current().nextInt(),
              ThreadLocalRandom.current().nextInt()
          ), TableColumn::asIntegerArray,
          (v) -> v.stream().map(Object::toString)
              .collect(Collectors.joining(",", "{", "}"))
      ),
      "long_array", new Field<>( "bigint[]",
          () -> List.of(
              ThreadLocalRandom.current().nextLong(),
              ThreadLocalRandom.current().nextLong()
          ), TableColumn::asLongArray,
          (v) -> v.stream().map(Object::toString)
              .collect(Collectors.joining(",", "{", "}"))
      ),
      "double_array", new Field<>( "numeric[]",
          () -> List.of(
              ThreadLocalRandom.current().nextDouble(),
              ThreadLocalRandom.current().nextDouble()
          ), TableColumn::asDoubleArray,
          (v) -> v.stream().map(Object::toString)
              .collect(Collectors.joining(",", "{", "}"))
      ),
      "string_array", new Field<>( "text[]",
          () -> List.of(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString()
          ), TableColumn::asStringArray,
          (v) -> v.stream().collect(Collectors.joining(",", "{", "}"))
      ),
      "bigdecimal", new Field<>( "numeric(12, 12)",
          () -> BigDecimal.valueOf(
              ThreadLocalRandom.current().nextDouble()
          ).setScale(12, BigDecimal.ROUND_HALF_UP), TableColumn::asBigDecimal,
          (v) -> v.toString()
      )
  );

  @Test
  public void testMessages() throws SQLException, InterruptedException {
    PgConnectionFactory connectionFactory = new PgConnectionFactory(
        postgresqlContainer.getJdbcUrl(),
        postgresqlContainer.getUsername(),
        postgresqlContainer.getPassword()
    );

    try (Connection queryConnection = connectionFactory.getConnection()) {

      String sqlFields = fields.entrySet().stream().map(
          f -> String.format("\"%s\" %s", f.getKey(), f.getValue().type)
      ).collect(Collectors.joining(",\n", "", ",\n"));

      try (Statement statement = queryConnection.createStatement()) {
        statement.execute(String.format(createTable, sqlFields));
      }

      ExecutorService executorService = Executors.newCachedThreadPool();
      DbChangeConsumer changeConsumer = new DbChangeConsumer();
      CdcEngine cdcEngine = new CdcEngine(
          "testpublicationname",
          "testslotname",
          changeConsumer,
          connectionFactory
      );
      executorService.submit(cdcEngine);

      while (!changeConsumer.isStarted()) {
        Thread.sleep(100L);
      }

      for (Map.Entry<String, Field<?>> field : fields.entrySet()) {
        Field<?> fieldDescriptor = field.getValue();
        String stringValue = fieldDescriptor.generateString();

        try (Statement statement = queryConnection.createStatement()) {
          statement.execute(
              String.format("INSERT INTO test (\"%s\") VALUES ('%s')", field.getKey(), stringValue)
          );
          statement.execute(
              String.format("INSERT INTO test (\"%s\") VALUES (%s)", field.getKey(), "null")
          );
        }

        while (changeConsumer.getHandled().size() < 2) {
          Thread.sleep(100L);
        }

        TableRecord tableRecord = changeConsumer.getHandled().get(0);
        TableRecord nullRecord = changeConsumer.getHandled().get(1);

        changeConsumer.clear();

        Assert.assertTrue(tableRecord.is(MessageType.INSERT));
        Assert.assertTrue(nullRecord.is(MessageType.INSERT));

        String gotValueString = tableRecord.getColumn(field.getKey())
            .map(fieldDescriptor::getString).orElseThrow();

        Assert.assertEquals(stringValue, gotValueString);

        Optional<TableColumn> column = nullRecord.getColumn(field.getKey());
        Assert.assertTrue(column.isPresent());
        Assert.assertTrue(column.get().isNull());
      }
    }
  }
}
