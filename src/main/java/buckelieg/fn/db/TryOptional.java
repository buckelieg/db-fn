/*
 * Copyright 2016- Anatoly Kutyakov
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
package buckelieg.fn.db;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;

/**
 * A container for result of the computation which might throw an exception.
 *
 * @param <T> value type
 */
@ParametersAreNonnullByDefault
public final class TryOptional<T> {

    private final T value;
    private final Throwable exception;

    private TryOptional(@Nullable T value) {
        this.value = value;
        this.exception = null;
    }

    private TryOptional(Throwable exception) {
        this.value = null;
        this.exception = Objects.requireNonNull(exception);
    }

    /**
     * Constructs {@link TryOptional} from {@link TrySupplier}.
     * This container holds either:
     * a value - if one is provided by supplier
     * an exception - whenever supplier throws it
     *
     * @param supplier value supplier
     * @return a {@link TryOptional} representing computation results
     * @throws NullPointerException if the supplier is null
     */
    @Nonnull
    public static <T> TryOptional<T> of(TrySupplier<T, Throwable> supplier) {
        Objects.requireNonNull(supplier, "Value supplier must be provided");
        try {
            return new TryOptional<>(supplier.get());
        } catch (Throwable t) {
            return new TryOptional<>(t);
        }
    }

    /**
     * Test if this container is an exception.
     *
     * @return true if this container represents an exception, false - otherwise
     */
    public boolean isException() {
        return exception != null;
    }

    /**
     * If this container represents a value, return the underlying one.
     * Otherwise throw an exception.
     *
     * @return underlying value
     */
    @Nullable
    public T get() throws Throwable {
        if (isException()) {
            throw exception;
        }
        return value;
    }

    /**
     * Get underlying value or throw an exception wrapped in {@link RuntimeException}
     *
     * @return underlying value
     */
    @Nullable
    public T getUnchecked() {
        try {
            return get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Shorthand method to be used in cases which we want to obtain a value.
     * Possible exception is omitted so that value will be null and returned optional is empty.
     *
     * @return a value (as an {@link Optional}) of the computation ignoring possible exception
     * @see Optional
     */
    @Nonnull
    public Optional<T> toOptional() {
        return Optional.ofNullable(value);
    }

    /**
     * Method used to obtain a value as {@link Optional}.
     * The difference between this method and {@link #toOptional()} is that if this TryOptional is an exception
     * then {@link RuntimeException} is thrown.
     *
     * @return a value (as an {@link Optional}) of the computation regarding possible exception.
     * @see #getUnchecked()
     */
    @Nonnull
    public Optional<T> getOptional() {
        return Optional.ofNullable(getUnchecked());
    }

    /**
     * Maps this {@link TryOptional} to another one with provided mapper.
     * Whenever this container represents an exception and mapper throws an exception of a new type - old exception is overwritten
     *
     * @param mapper a value mapper.
     * @return a {@link TryOptional} with mapped value or with (possibly new type of) exception.
     * @throws NullPointerException if the mapper is null
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public <U> TryOptional<U> map(TryFunction<? super T, ? extends U, Throwable> mapper) {
        return isException() ? (TryOptional<U>) this : of(() -> mapper.apply(value));
    }

    /**
     * Exceptional case handler which is expected to provide a new {@code TryOptional} with recovered state.
     *
     * @param handler a {@link TryFunction} that handles this exception
     * @return a new {@link TryOptional} with exception processing results.
     * @throws NullPointerException if the handler function is null
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public <U> TryOptional<U> recover(TryFunction<Throwable, U, Throwable> handler) {
        return isException() ? of(() -> Objects.requireNonNull(handler).apply(exception)) : (TryOptional<U>) this;
    }

    /**
     * Exception consumer. This is expected to be terminal operation.
     *
     * @param handler a {@link TryConsumer} for exception
     * @throws NullPointerException if handler is null
     */
    public <E extends Throwable> void onException(TryConsumer<Throwable, E> handler) throws E {
        if (isException()) Objects.requireNonNull(handler).accept(exception);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TryOptional<?> that = (TryOptional<?>) o;

        return (value != null ? value.equals(that.value) : that.value == null) && (isException() ? exception.equals(that.exception) : that.exception == null);
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (isException() ? exception.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s.%s(%s)", getClass().getSimpleName(), isException() ? "exception" : "value", isException() ? exception : value);
    }
}