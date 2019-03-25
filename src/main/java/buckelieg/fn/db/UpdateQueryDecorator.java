package buckelieg.fn.db;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

@ParametersAreNonnullByDefault
final class UpdateQueryDecorator extends UpdateQuery {

    private TrySupplier<Connection, SQLException> connectionSupplier;
    private String query;
    private Object[][] batch;
    private boolean isLarge = false;
    private boolean isBatch = false;
    private boolean isPoolable = false;
    private boolean isEscaped = true;
    private int timeout = 0;

    UpdateQueryDecorator(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object[]... batch) {
        super(connectionSupplier, query, batch);
        this.connectionSupplier = connectionSupplier;
        this.query = query;
        this.batch = batch;
    }

    @Nonnull
    @Override
    public Long execute() {
        return setStatementParameters(new UpdateQuery(connectionSupplier, query, batch)).execute();
    }

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler) {
        return execute(generatedValuesHandler, new int[0]);
    }

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler, int... colIndices) {
        return setStatementParameters(new UpdateQuery(colIndices, connectionSupplier, query, batch)).execute(generatedValuesHandler, colIndices);
    }

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler, String... colNames) {
        return setStatementParameters(new UpdateQuery(colNames, connectionSupplier, query, batch)).execute(generatedValuesHandler, colNames);
    }

    @Nonnull
    @Override
    public Update large(boolean isLarge) {
        this.isLarge = isLarge;
        return this;
    }

    @Nonnull
    @Override
    public Update batched(boolean isBatch) {
        this.isBatch = isBatch;
        return this;
    }

    @Nonnull
    @Override
    public Update timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    @Nonnull
    @Override
    public Update poolable(boolean poolable) {
        this.isPoolable = poolable;
        return this;
    }

    @Nonnull
    @Override
    public Update escaped(boolean escapeProcessing) {
        this.isEscaped = escapeProcessing;
        return this;
    }

    private Update setStatementParameters(Update query) {
        return query.timeout(timeout).poolable(isPoolable).escaped(isEscaped).batched(isBatch).large(isLarge);
    }

}