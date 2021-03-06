/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "item.h"

class ItemMetaData;
class StoredValue;

/**
 * An abstract class for doing conflict resolution for documents sent from
 * different datacenters.
 */
class ConflictResolution {
public:
    ConflictResolution() {}

    virtual ~ConflictResolution() {}

    /**
     * Resolves a conflict between two documents.
     *
     * @param v the local document meta data
     * @param meta the remote document's meta data
     * @param meta_dataype datatype of remote document
     * @param isDelete the flag indicating if conflict resolution is
     *                 for delete operations
     * @return true is the remote document is the winner, false otherwise
     */
    virtual bool resolve(const StoredValue& v,
                         const ItemMetaData& meta,
                         const protocol_binary_datatype_t meta_datatype,
                         bool isDelete = false) const = 0;

};

class RevisionSeqnoResolution : public ConflictResolution {
public:
    bool resolve(const StoredValue& v,
                 const ItemMetaData& meta,
                 const protocol_binary_datatype_t meta_datatype,
                 bool isDelete = false) const override;
};

class LastWriteWinsResolution : public ConflictResolution {
public:
    bool resolve(const StoredValue& v,
                 const ItemMetaData& meta,
                 const protocol_binary_datatype_t meta_datatype,
                 bool isDelete = false) const override;
};
