package com.github.germanosin.postgresql.cdc;

import static com.github.germanosin.postgresql.cdc.wal.MessageType.DELETE;
import static com.github.germanosin.postgresql.cdc.wal.MessageType.INSERT;
import static com.github.germanosin.postgresql.cdc.wal.MessageType.UPDATE;

import com.github.germanosin.postgresql.cdc.wal.Message;
import com.github.germanosin.postgresql.cdc.wal.MessageType;
import com.github.germanosin.postgresql.cdc.wal.TableRecord;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

@Slf4j
public class CdcEngine implements Runnable {
  private static final String PG_REPLICATION_OUTPUT_PLUGIN = "pgoutput";

  private final String publicationName;
  private final String slotName;
  private final int maxBatchSize;

  private final CdcConsumer cdcConsumer;
  private final PgWalMessageDecoder decoder;
  private final PgConnectionFactory connectionFactory;

  public CdcEngine(String publicationName, String slotName, CdcConsumer cdcConsumer,
                   PgConnectionFactory connectionFactory) {
    this(publicationName, slotName, 100, cdcConsumer,
        new PgWalMessageDecoder(), connectionFactory
    );
  }

  public CdcEngine(String publicationName, String slotName, int maxBatchSize,
                   CdcConsumer cdcConsumer,
                   PgWalMessageDecoder decoder, PgConnectionFactory connectionFactory) {
    this.publicationName = publicationName;
    this.slotName = slotName;
    this.maxBatchSize = maxBatchSize;
    this.cdcConsumer = cdcConsumer;
    this.decoder = decoder;
    this.connectionFactory = connectionFactory;
  }

  @Override
  public void run() {
    final Properties replicationSlotOptions = new Properties();
    replicationSlotOptions.putAll(Map.of(
        "proto_version", "1",
        "publication_names", this.publicationName
    ));

    while (!Thread.interrupted()) {
      final Connection replicationConnection = connectionFactory.getConnection(true);

      try {
        final PGConnection pgReplicationConnection =
            replicationConnection.unwrap(PGConnection.class);

        registerReplicationSlot(replicationConnection, pgReplicationConnection);
        registerPublication(replicationConnection);

        final ChainedLogicalStreamBuilder streamBuilder =
            pgReplicationConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(this.slotName)
                .withSlotOptions(replicationSlotOptions);

        try (final PGReplicationStream stream = streamBuilder.start()) {

          cdcConsumer.markStarted();
          List<TableRecord> batch = null;

          while (true) {
            if (Thread.interrupted()) {
              log.warn("Subscriber thread interrupted while processing WAL messages");
              Thread.currentThread().interrupt();
              return;
            }

            final ByteBuffer buffer = stream.readPending();

            if (buffer == null) {
              TimeUnit.MILLISECONDS.sleep(10L);
              continue;
            }

            log.debug("processing LSN: {}", stream.getLastReceiveLSN());

            final Optional<Message> decodedMessage = decoder.decode(buffer, connectionFactory);

            if (decodedMessage.isPresent()) {
              Message walMessage = decodedMessage.get();

              if (walMessage.is(MessageType.BEGIN)) {
                batch = new ArrayList<>();
              } else if (walMessage.is(MessageType.COMMIT)) {
                if (batch != null && !batch.isEmpty()) {
                  cdcConsumer.handleBatch(batch);
                }
                batch = null;
              } else if (walMessage.any(List.of(INSERT, UPDATE, DELETE))) {
                if (walMessage instanceof TableRecord) {
                  if (batch != null) {
                    batch.add((TableRecord) walMessage);
                    if (batch.size() > maxBatchSize) {
                      cdcConsumer.handleBatch(batch);
                      batch.clear();
                    }
                  } else {
                    cdcConsumer.handleBatch(List.of((TableRecord) walMessage));
                  }
                }
              }
            }

            if (batch == null) {
              LogSequenceNumber lastReceiveLsn = stream.getLastReceiveLSN();
              stream.setAppliedLSN(lastReceiveLsn);
              stream.setFlushedLSN(lastReceiveLsn);
              stream.forceUpdateStatus();
            }
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (final Exception e) {
        log.error("Error occurred while subscribing", e);
      } finally {
        try {
          replicationConnection.close();
        } catch (final SQLException e) {
          log.error("Error while trying to close JDBC replication connection", e);
        }
      }

      log.debug("Released a lock, waiting 10 seconds for next iteration");
      try {
        TimeUnit.SECONDS.sleep(10L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private void registerReplicationSlot(final Connection connection,
                                       final PGConnection replicationConnection)
      throws SQLException {
    final String existsQuery =
        "SELECT EXISTS (SELECT slot_name FROM pg_replication_slots WHERE slot_name = ?)";

    try (final PreparedStatement statement = connection.prepareStatement(existsQuery)) {
      statement.setString(1, this.slotName);

      try (final ResultSet resultSet = statement.executeQuery()) {
        resultSet.next();
        if (!resultSet.getBoolean(1)) {
          log.debug("Creating replication slot with name {}", this.slotName);
          replicationConnection.getReplicationAPI()
              .createReplicationSlot()
              .logical()
              .withSlotName(this.slotName)
              .withOutputPlugin(PG_REPLICATION_OUTPUT_PLUGIN)
              .make();
        }
      }
    }

    log.debug("Replication slot {} registered", this.slotName);
  }

  private void registerPublication(final Connection connection)
      throws SQLException {

    final String existsQuery = "SELECT EXISTS (SELECT oid FROM pg_publication WHERE pubname = ?)";

    try (final PreparedStatement existsStatement = connection.prepareStatement(existsQuery)) {
      existsStatement.setString(1, this.publicationName);

      try (final ResultSet resultSet = existsStatement.executeQuery()) {
        resultSet.next();
        if (!resultSet.getBoolean(1)) {
          log.debug("Creating publication with name {}", this.publicationName);
          try (final Statement publicationStatement = connection.createStatement()) {
            publicationStatement.execute(
                String.format(
                    "CREATE PUBLICATION %s FOR ALL TABLES",
                    this.publicationName
                )
            );
          }
        } else {
          log.debug("Publication with name {} is already created", this.publicationName);
        }
      }
    }
    log.debug("Publication {} registered", this.publicationName);
  }
}