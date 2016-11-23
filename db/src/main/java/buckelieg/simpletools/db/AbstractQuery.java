/*
* Copyright 2016 Anatoly Kutyakov
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package buckelieg.simpletools.db;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

abstract class AbstractQuery<T extends Statement> {

    private static final Logger LOG = Logger.getLogger(AbstractQuery.class);

    final T statement;

    AbstractQuery(T statement) {
        this.statement = Objects.requireNonNull(statement, "Statement must not be null");
    }

    protected void close() {
        try {
            if (statement != null && !statement.isClosed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Closing statement '%s'", statement));
                }
                statement.close(); // by JDBC spec: subsequently closes all result sets opened by this statement
            }
        } catch (SQLException e) {
            logSQLException(String.format("Could not close the statement '%s'", statement), e);
        }
    }

    final void logSQLException(String prepend, SQLException e) {
        LOG.warn(String.format(prepend == null || prepend.isEmpty() ? "Caught '%s'" : prepend + " due to '%s'", e.getMessage()));
        if (LOG.isDebugEnabled()) {
            LOG.debug(e);
        }
    }
}
