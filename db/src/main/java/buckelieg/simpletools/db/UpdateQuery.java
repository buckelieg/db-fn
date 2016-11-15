package buckelieg.simpletools.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

final class UpdateQuery extends AbstractQuery implements Update {

    UpdateQuery(Statement statement) {
        super(statement);
    }

    @Override
    public Integer execute() {
        int rows = 0;
        try {
            rows = ((PreparedStatement) statement).executeUpdate();
        } catch (SQLException e) {
            logSQLException(String.format("Could not execute statement '%s'", statement), e);
        } finally {
            close();
        }
        return rows;
    }
}
