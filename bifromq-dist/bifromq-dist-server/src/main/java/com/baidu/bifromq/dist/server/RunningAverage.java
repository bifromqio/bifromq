/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.baidu.bifromq.dist.server;

/**
 * A class to compute the running average of a set number of most recent values logged. It uses a circular buffer to
 * maintain a window of the most recent values and computes the average efficiently as new values are added.
 */
public class RunningAverage {
    private final int[] window;
    private long total;  // Changed from int to long to handle larger sums
    private int count;
    private int estimate;

    /**
     * Constructs a RunningAverage with a specified window size.
     *
     * @param windowSize The size of the window for which the average is to be maintained. Must be greater than zero.
     * @throws AssertionError if windowSize is less than or equal to zero.
     */
    public RunningAverage(int windowSize) {
        assert windowSize > 0 : "Window size must be greater than 0";
        window = new int[windowSize];
        total = 0;
        count = 0;
    }

    /**
     * Logs a new value to the running average calculation. This method updates the circular buffer with the new value,
     * adjusts the total sum, and recomputes the average.
     *
     * @param value The new value to log.
     */
    public synchronized void log(int value) {
        int idx = count % window.length;
        total += value - window[idx];
        window[idx] = value;
        if (++count > window.length) {  // Increment count and check if it exceeds window length
            count = window.length;      // Keep count at window size to avoid it growing unbounded
        }
        estimate = (int) (total / count);
    }

    /**
     * Returns the current estimate of the average of the values in the window.
     *
     * @return The current average of the values.
     */
    public int estimate() {
        return estimate;
    }
}