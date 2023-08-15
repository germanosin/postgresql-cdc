package com.github.germanosin.postgresql.cdc.exception;

import java.sql.SQLException;

public class CannotGetJdbcConnectionException extends RuntimeException {
  public CannotGetJdbcConnectionException(String msg) {
    super(msg);
  }

  /**
   * Constructor for CannotGetJdbcConnectionException.
   * @param msg the detail message
   * @param ex the root cause SQLException
   */
  public CannotGetJdbcConnectionException(String msg, SQLException ex) {
    super(msg, ex);
  }
}
