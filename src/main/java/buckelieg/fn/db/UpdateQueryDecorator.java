package buckelieg.fn.db;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

@ParametersAreNonnullByDefault
final class UpdateQueryDecorator extends UpdateQuery {

    UpdateQueryDecorator(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object[]... batch) {
        super(connectionSupplier, query, batch);
    }

    @Nonnull
    @Override
    public Long execute() {
        return setQueryParameters(new UpdateQuery(connectionSupplier, query, batch)).execute();
    }

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler) {
        return execute(generatedValuesHandler, new int[0]);
    }

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler, int... colIndices) {
        return setQueryParameters(new UpdateQuery(colIndices, connectionSupplier, query, batch)).execute(generatedValuesHandler);
    }

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler, String... colNames) {
        return setQueryParameters(new UpdateQuery(colNames, connectionSupplier, query, batch)).execute(generatedValuesHandler);
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

    private Update setQueryParameters(Update query) {
        return query.timeout(timeout).poolable(isPoolable).escaped(isEscaped).batched(isBatch).large(isLarge).transacted(isolationLevel);
    }

}