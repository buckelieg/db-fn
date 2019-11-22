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

import static java.util.Objects.requireNonNull;

/**
 * Represents an operation that accepts a single input argument and returns no
 * result with an optional exception.
 *
 * @param <T> the type of the input to the operation
 * @param <E> the type of the possible exception
 */
@SuppressWarnings("unchecked")
@FunctionalInterface
public interface TryConsumer<T, E extends Throwable> {

    /**
     * Performs this operation on the given argument which might throw an exception.
     *
     * @param t the input argument
     * @throws E an exception
     */
    void accept(T t) throws E;

    /**
     * Returns reference of lambda expression.
     *
     * @param tryConsumer a consumer
     * @return {@link TryConsumer} reference
     * @throws NullPointerException if tryConsumer is null
     */
    static <T, E extends Throwable> TryConsumer<T, E> of(TryConsumer<T, E> tryConsumer) {
        return requireNonNull(tryConsumer);
    }

    /**
     * Returns a composed {@code TryConsumer} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, then corresponding exception is thrown.
     * If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code TryConsumer} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws E                    an exception
     * @throws NullPointerException if {@code after} is null
     */
    default TryConsumer<T, E> andThen(TryConsumer<? super T, E> after) throws E {
        try {
            return (T t) -> {
                accept(t);
                requireNonNull(after).accept(t);
            };
        } catch (Throwable t) {
            throw (E) t;
        }
    }

    /**
     * Returns a composed {@code TryConsumer} that performs, in sequence, this
     * operation is preceded by the {@code before} operation. If performing either
     * operation throws an exception, then corresponding exception is thrown.
     * If performing this operation throws an exception,
     * the {@code before} operation will not be performed.
     *
     * @param before the operation to perform before this operation
     * @return a composed {@code TryConsumer} that performs in sequence this
     * operation followed by the {@code before} operation
     * @throws E                    an exception
     * @throws NullPointerException if {@code before} is null
     */
    default TryConsumer<T, E> compose(TryConsumer<? super T, E> before) throws E {
        try {
            return (T t) -> {
                requireNonNull(before).accept(t);
                accept(t);
            };
        } catch (Throwable t) {
            throw (E) t;
        }
    }

}
