/*
* Copyright 2016-2017 Anatoly Kutyakov
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

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;

@NotThreadSafe
@ParametersAreNonnullByDefault
final class ProcedureCallQuery extends SelectQuery implements ProcedureCall {

    private TryFunction<CallableStatement, ?, SQLException> mapper;
    private Consumer consumer;

    ProcedureCallQuery(CallableStatement statement) {
        super(statement);
    }

    @Nonnull
    @Override
    public <T> Select invoke(TryFunction<CallableStatement, T, SQLException> mapper, Consumer<T> consumer) {
        this.mapper = Objects.requireNonNull(mapper, "Mapper must be provided");
        this.consumer = Objects.requireNonNull(consumer, "Consumer must be provided");
        return this;
    }

    @Override
    protected void doExecute() {
        jdbcTry(() -> {
            if (statement.execute()) {
                rs = statement.getResultSet();
            }
        });
    }

    @SuppressWarnings("unchecked")
    protected boolean doHasNext() {
        return jdbcTry(() -> {
            boolean moved = super.doHasNext();
            if (!moved) {
                if (statement.getMoreResults()) {
                    if (rs != null && !rs.isClosed()) {
                        rs.close();
                    }
                    rs = statement.getResultSet();
                    return super.doHasNext();
                }
                try {
                    if (mapper != null && consumer != null) {
                        consumer.accept(mapper.apply((CallableStatement) statement));
                    }
                } finally {
                    close();
                }
            }
            return moved;
        });
    }

}
