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

#include "item_eviction.h"
#include "item.h"

#include <gsl/gsl>

ItemEviction::ItemEviction() {
}

void ItemEviction::addValueToFreqHistogram(uint8_t v) {
    freqHistogram.addValue(v);
}

uint64_t ItemEviction::getFreqHistogramValueCount() const {
    return freqHistogram.getValueCount();
}

void ItemEviction::reset() {
    freqHistogram.reset();
    requiredToUpdateInterval = 1;
}

uint16_t ItemEviction::getFreqThreshold(double percentage) const {
    uint16_t freq = gsl::narrow<uint16_t>(
            freqHistogram.getValueAtPercentile(percentage));
    return freq;
}

uint8_t ItemEviction::convertFreqCountToNRUValue(uint8_t probCounter) {
    /*
     * The probabilistic counter mimics a unsigned 16-bit counter and therefore
     * can be 'incremented' approximately 65k times before it will become
     * saturated.  Therefore the 4 states could be mapped as follows:
     *
     * 0%-24% of 65K   => 3 (coldest)
     * 25%-49% of 65K  => 2
     * 50%-74% of 65K  => 1
     * 75%-100% of 65K => 0 (hottest)
     *
     * However with 2-bit_lru eviction policy we initialise new items to the
     * state '2' but with the hifi_mfu eviction policy we initialise new items
     * with the value 64 (which corresponds to the counter value after
     * approximately 5% of 65K 'increments').
     *
     * Therefore to ensure that new items are not mapped to the NRU coldest
     * state we modify the mapping as follows:
     *
     * 0%-4% of 65K    => 3 (coldest)
     * 5%-32% of 65K   => 2
     * 33%-66% of 65K  => 1
     * 67%-100% of 65K => 0 (hottest)
     *
     * This translates into the following counter value ranges.  Note although
     * each of the 4 states have 25% of the 256 available values (i.e. 64) the
     * percentages are not equal.  This is because initially - when the
     * counter is low - it is easier to increment the counter.
     *
     * 0%-4% of 65K   => 0-63 of 255 (coldest)
     * 5%-32% of 65K  => 64-127 of 255
     * 33%-66% of 65K => 128-191 of 255
     * 67%-100% of 65K => 192 (hottest)
     *
     */
    if (probCounter >= 192) {
        return MIN_NRU_VALUE; /* 0 - the hottest */
    } else if (probCounter >= 128) {
        return 1;
    } else if (probCounter >= 64) {
        return INITIAL_NRU_VALUE; /* 2 */
    }
    return MAX_NRU_VALUE; /* 3 - the coldest */
}

void ItemEviction::copyToHistogram(HdrHistogram& hist) {
    HdrHistogram::Iterator iter{
            freqHistogram.makeLinearIterator(valueUnitsPerBucket)};
    while (auto result = freqHistogram.getNextValueAndCount(iter)) {
        hist.addValueAndCount(result->first, result->second);
    }
}
