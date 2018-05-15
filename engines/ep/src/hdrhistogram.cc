/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "hdrhistogram.h"

#include <cstdlib> // Required due to the use of free

// Custom deleter for the hdr_histogram struct.
void HdrHistogram::HdrDeleter::operator()(struct hdr_histogram* val) {
    free(val);
}

HdrHistogram::HdrHistogram(uint64_t lowestTrackableValue,
                           uint64_t highestTrackableValue,
                           int significantFigures) :
                               maxValue(highestTrackableValue+1) {
    struct hdr_histogram* hist;
    // We add a bias of +1 to the lowest and highest trackable value
    // because we add +1 to all values we store in the histogram (as this
    // allows us to record the value 0).
    hdr_init(lowestTrackableValue + 1,
             highestTrackableValue + 1, // Add one because all values
             significantFigures,
             &hist); // Pointer to initialise
    histogram.reset(hist);
}

void HdrHistogram::addValue(uint64_t v) {
    // A hdr_histogram cannot store 0, therefore we add a bias of +1.
    int64_t vBiased = v + 1;
    hdr_record_value(histogram.get(), vBiased);
}

void HdrHistogram::addValueAndCount(uint64_t v, uint64_t count) {
    // A hdr_histogram cannot store 0, therefore we add a bias of +1.
    int64_t vBiased = v + 1;
    hdr_record_values(histogram.get(), vBiased, count);
}

uint64_t HdrHistogram::getValueCount() const {
    return histogram->total_count;
}

void HdrHistogram::reset() {
    hdr_reset(histogram.get());
}

uint64_t HdrHistogram::getValueAtPercentile(double percentage) const {
    // We added the bias of +1 to the input value (see
    // addValueToFreqHistogram).  Therefore need to minus the bias
    // before returning the value from the histogram.
    uint64_t value = hdr_value_at_percentile(histogram.get(), percentage);
    return (value - 1);
}

HdrHistogram::Iterator HdrHistogram::makeLinearIterator(
        int64_t valueUnitsPerBucket) const {
    HdrHistogram::Iterator iter;
    hdr_iter_linear_init(&iter, histogram.get(), valueUnitsPerBucket);
    return iter;
}

HdrHistogram::Iterator HdrHistogram::makeLogIterator(
        int64_t valueUnitsFirstBucket, double logBase) const {
    HdrHistogram::Iterator iter;
    hdr_iter_log_init(&iter, histogram.get(), valueUnitsFirstBucket, logBase);
    return iter;
}

boost::optional<std::pair<uint64_t, uint64_t>>
HdrHistogram::getNextValueAndCount(Iterator& iter) const {
    boost::optional<std::pair<uint64_t, uint64_t>> valueAndCount;
    if (hdr_iter_next(&iter)) {
        uint64_t value = iter.value;
        uint64_t count = hdr_count_at_value(histogram.get(), value);
        // We added the bias of +1 to the input value (see
        // addValueToFreqHistogram).  Therefore need to minus the bias
        // before returning value.
        return valueAndCount = std::make_pair(value - 1, count);
    } else {
        return valueAndCount;
    }
}
