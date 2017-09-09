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
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A container for result of the computation which might throw an exception.
 *
 * @param <T> value type
 * @param <E> an exception type
 */
@ParametersAreNonnullByDefault
public final class TryOptional<T, E extends Throwable> {

    private final T value;
    private final E exception;

    private TryOptional(@Nullable T value) {
        this.value = value;
        this.exception = null;
    }

    private TryOptional(E exception) {
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
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T, E extends Throwable> TryOptional<T, E> of(TrySupplier<T, E> supplier) {
        Objects.requireNonNull(supplier, "Value supplier must be provided");
        try {
            return new TryOptional<>(supplier.get());
        } catch (Throwable t) {
            return new TryOptional<>((E) t);
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
     * @throws E an exception
     */
    @Nullable
    public T get() throws E {
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
     * Maps this {@link TryOptional} to another one with provided mapper.
     * Whenever this container represents an exception and mapper throws an exception of a new type - old exception is overwritten
     *
     * @param mapper a value mapper.
     * @return a {@link TryOptional} with mapped value or with (possibly new type of) exception.
     * @throws NullPointerException if the mapper is null
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public <U, UE extends Throwable> TryOptional<U, UE> map(TryFunction<? super T, ? extends U, UE> mapper) {
        return isException() ? new TryOptional<>((UE) exception) : of(() -> mapper.apply(value));
    }

    /**
     * Creates a stream using {@code value} mapper function
     * (as the (@code value} in this container could be of any type including collections).
     *
     * @param streamer a function that handle process of creating a {@code stream} from the {@code value}
     * @return a (@code stream} from the {@code value}
     * @throws NullPointerException if stream supplier is null
     * @see Function
     */
    @Nonnull
    public Stream<T> stream(Function<T, Stream<T>> streamer) {
        return Optional.ofNullable(Objects.requireNonNull(streamer).apply(value)).orElse(Stream.empty());
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
    public <U, UE extends Throwable> TryOptional<U, UE> recover(TryFunction<E, U, UE> handler) {
        return isException() ? new TryOptional<>((U) value) : of(() -> handler.apply(exception));
    }

    /**
     * Exception consumer. This is expected to be terminal operation.
     *
     * @param handler a {@link TryConsumer} for exception
     * @throws UE                   an optional exception. To make exception optional it must derive from {@link RuntimeException}
     * @throws NullPointerException if handler is null
     */
    public <UE extends Throwable> void onException(TryConsumer<E, UE> handler) throws UE {
        if (isException()) Objects.requireNonNull(handler).accept(exception);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TryOptional<?, ?> that = (TryOptional<?, ?>) o;

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