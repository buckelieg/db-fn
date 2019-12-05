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
 * Two-argument function with returned result that throws an exception
 *
 * @param <I1> first input argument type
 * @param <I2> second input argument type
 * @param <O>  result type
 * @param <E>  an exception type thrown
 */
@FunctionalInterface
public interface TryBiFunction<I1, I2, O, E extends Throwable> {

    /**
     * Returns reference of lambda expression.
     *
     * @param tryBiFunction a function
     * @return lambda as {@link TryBiFunction} reference
     * @throws NullPointerException if tryBiFunction is null
     */
    static <I1, I2, O, E extends Throwable> TryBiFunction<I1, I2, O, E> of(TryBiFunction<I1, I2, O, E> tryBiFunction) {
        return requireNonNull(tryBiFunction);
    }

    /**
     * Represents some two-argument function which might throw an Exception
     *
     * @param input1 first argument
     * @param input2 second argument
     * @return output
     * @throws E an exception
     */
    O apply(I1 input1, I2 input2) throws E;

}
