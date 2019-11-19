package buckelieg.fn.db;

import java.sql.Connection;

/**
 * Transaction isolation level
 *
 * @see Connection#TRANSACTION_READ_UNCOMMITTED
 * @see Connection#TRANSACTION_READ_COMMITTED
 * @see Connection#TRANSACTION_REPEATABLE_READ
 * @see Connection#TRANSACTION_SERIALIZABLE
 */
public enum TransactionIsolation {

    READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

    int level;

    TransactionIsolation(int level) {
        this.level = level;
    }
}
