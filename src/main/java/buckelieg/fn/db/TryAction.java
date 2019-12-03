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
 * An arbitrary action that might throw an exception
 *
 * @param <E> exception type
 */
@SuppressWarnings("unchecked")
@FunctionalInterface
public interface TryAction<E extends Throwable> {

    /**
     * Performs an action with possible exceptional result
     *
     * @throws E an exception
     */
    void doTry() throws E;

    /**
     * Returns reference of lambda expression.
     *
     * @param tryAction an action
     * @return {@link TryAction} reference
     * @throws NullPointerException if tryAction is null
     */
    static <E extends Throwable> TryAction<E> of(TryAction<E> tryAction) {
        return requireNonNull(tryAction);
    }

    /**
     * Returns a composed {@code TryAction} that performs, in sequence, this
     * operation is followed by the {@code after} operation. If performing either
     * operation throws an exception, then corresponding exception is thrown.
     * If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code TryAction} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws E                    an exception
     * @throws NullPointerException if {@code after} is null
     */
    default TryAction<E> andThen(TryAction<E> after) throws E {
        requireNonNull(after);
        try {
            return () -> {
                doTry();
                after.doTry();
            };
        } catch (Throwable t) {
            throw (E) t;
        }
    }
    /**
     * Returns a composed {@code TryAction} that performs, in sequence, this
     * operation is preceded by the {@code before} operation. If performing either
     * operation throws an exception, then corresponding exception is thrown.
     * If performing this operation throws an exception,
     * the {@code before} operation will not be performed.
     *
     * @param before the operation to perform before this operation
     * @return a composed {@code TryAction} that performs in sequence this
     * operation followed by the {@code before} operation
     * @throws E                    an exception
     * @throws NullPointerException if {@code before} is null
     */
    default TryAction<E> compose(TryAction<E> before) throws E {
        requireNonNull(before);
        try {
            return () -> {
                before.doTry();
                doTry();
            };
        } catch (Throwable t) {
            throw (E) t;
        }
    }

}
