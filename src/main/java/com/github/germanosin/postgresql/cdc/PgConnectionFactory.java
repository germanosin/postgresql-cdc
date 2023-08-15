package com.github.germanosin.postgresql.cdc;

import com.github.germanosin.postgresql.cdc.exception.CannotGetJdbcConnectionException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import org.postgresql.PGProperty;

@RequiredArgsConstructor
public class PgConnectionFactory {

  private final String url;
  private final String userName;
  private final String password;

  public Connection getConnection() {
    return getConnection(false);
  }

  public Connection getConnection(final boolean replicationMode) {
    final Properties props = new Properties();

    PGProperty.USER.set(props, userName);
    PGProperty.PASSWORD.set(props, password);
    if (replicationMode) {
      PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "11.0");
      PGProperty.REPLICATION.set(props, "database");
      PGProperty.PREFER_QUERY_MODE.set(props, "simple");
    }

    try {
      return DriverManager.getConnection(url, props);
    } catch (final SQLException ex) {
      throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
    } catch (final IllegalStateException ex) {
      throw new CannotGetJdbcConnectionException(
          "Failed to obtain JDBC Connection: " + ex.getMessage()
      );
    }
  }

}
