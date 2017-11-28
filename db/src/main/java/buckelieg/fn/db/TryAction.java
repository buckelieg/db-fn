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
package buckelieg.fn.db;

import java.util.Objects;

/**
 * An arbitrary action that might throw an exception
 *
 * @param <E> exception type
 */
@FunctionalInterface
public interface TryAction<E extends Throwable> {

    /**
     * Performs an action with possible exceptional result
     *
     * @throws E an exception
     */
    void doTry() throws E;

    /**
     * Returns a composed {@code TryAction} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
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
        Objects.requireNonNull(after);
        return () -> {
            doTry();
            after.doTry();
        };
    }

}
