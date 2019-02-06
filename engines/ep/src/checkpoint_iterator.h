/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#pragma once
#include <iterator>
#include <string>

/**
 * Class provides checkpoint iterator functionality.  It assumes the
 * container is index based (i.e. deque or vector).
 *
 * The functionality is such that if the iterator points to a nullptr in the
 * container then the iterator skips past it (either moving forward when in
 * the ++ operator; or moving backwards when in the -- operator.
 */

template <typename C>
class CheckpointIterator {
public:
    // The following type aliases are required to allow the iterator
    // to behave like a STL iterator so functions such as std::next work.
    using iterator_category = std::bidirectional_iterator_tag;
    using difference_type = typename C::difference_type;
    using value_type = typename C::value_type;
    using size_type = typename C::size_type;
    using pointer = typename C::pointer;
    using reference = typename C::reference;

    CheckpointIterator(C* c, typename C::iterator it) : container(c), iter(it) {
        while (!isAtEnd() && isNullElement()) {
            // while (isNullElement()) {
            ++iter;
        }
    }

    auto operator++() {
        ++iter;
        while (!isAtEnd() && isNullElement()) {
            // while (isNullElement()) {
            ++iter;
        }
        return *this;
    }

    auto operator--() {
        --iter;
        while (!isAtStart() && isNullElement()) {
            // while (isNullElement()) {
            --iter;
        }
        return *this;
    }

    auto operator==(CheckpointIterator ci) {
        return (ci.iter == iter && container == ci.container);
    }

    auto operator!=(CheckpointIterator ci) {
        return !operator==(ci);
    }

    auto& operator*() {
        return *iter;
    }

    auto& operator*() const {
        return *iter;
    }

    auto getCurrentIterator() const {
        return iter;
    }

private:
    /// Is the iterator currently pointing to the "end" element.
    bool isAtEnd() const {
        return (iter == container->end());
    }

    /// Is the iterator currently pointing to the first element,
    bool isAtStart() const {
        return (iter == container->begin());
    }

    /// Is the iterator currently pointing to a null element.
    bool isNullElement() const {
        return ((*iter).get() == nullptr);
    }

    /// reference_wrapper of the container being iterated over.
    C* container;
    /// The Container's standard iterator
    typename C::iterator iter;
};
