/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "config.h"

#include "stored-value.h"

#include "ep_time.h"
#include "item.h"
#include "objectregistry.h"
#include "stats.h"

#include <platform/cb_malloc.h>
#include <platform/compress.h>

const int64_t StoredValue::state_pending_seqno = -2;
const int64_t StoredValue::state_deleted_key = -3;
const int64_t StoredValue::state_non_existent_key = -4;
const int64_t StoredValue::state_temp_init = -5;
const int64_t StoredValue::state_collection_open = -6;

StoredValue::StoredValue(const Item& itm,
                         UniquePtr n,
                         EPStats& stats,
                         bool isOrdered)
    : value(itm.getValue()),
      chain_next_or_replacement(std::move(n)),
      cas(itm.getCas()),
      revSeqno(itm.getRevSeqno()),
      bySeqno(itm.getBySeqno()),
      lock_expiry_or_delete_time(0),
      exptime(itm.getExptime()),
      flags(itm.getFlags()),
      datatype(itm.getDataType()) {
    // Initialise bit fields
    setDeletedPriv(itm.isDeleted());
    setNewCacheItem(true);
    setOrdered(isOrdered);
    setNru(itm.getNRUValue());
    setResident(!isTempItem());
    setStale(false);
    // dirty initialised below

    // Placement-new the key which lives in memory directly after this
    // object.
    new (key()) SerialisedDocKey(itm.getKey());

    if (isTempInitialItem()) {
        markClean();
    } else {
        markDirty();
    }

    if (isTempItem()) {
        resetValue();
    }

    ObjectRegistry::onCreateStoredValue(this);
}

StoredValue::~StoredValue() {
    ObjectRegistry::onDeleteStoredValue(this);
}

StoredValue::StoredValue(const StoredValue& other, UniquePtr n, EPStats& stats)
    : value(other.value),
      chain_next_or_replacement(std::move(n)),
      cas(other.cas),
      revSeqno(other.revSeqno),
      bySeqno(other.bySeqno),
      lock_expiry_or_delete_time(other.lock_expiry_or_delete_time),
      exptime(other.exptime),
      flags(other.flags),
      datatype(other.datatype) {
    setDirty(other.isDirty());
    setDeletedPriv(other.isDeleted());
    setNewCacheItem(other.isNewCacheItem());
    setOrdered(other.isOrdered());
    setNru(other.getNru());
    setFreqCounterValue(other.getFreqCounterValue());
    setResident(other.isResident());
    setStale(false);
    // Placement-new the key which lives in memory directly after this
    // object.
    StoredDocKey sKey(other.getKey());
    new (key()) SerialisedDocKey(sKey);

    ObjectRegistry::onCreateStoredValue(this);
}

void StoredValue::setValue(const Item& itm) {
    if (isOrdered()) {
        return static_cast<OrderedStoredValue*>(this)->setValueImpl(itm);
    } else {
        return this->setValueImpl(itm);
    }
}

void StoredValue::ejectValue() {
    markNotResident();
}

void StoredValue::referenced() {
    uint8_t nru = getNru();
    if (nru > MIN_NRU_VALUE) {
        setNru(--nru);
    }
}

void StoredValue::setNRUValue(uint8_t nru_val) {
    if (nru_val <= MAX_NRU_VALUE) {
        setNru(nru_val);
    }
}

uint8_t StoredValue::incrNRUValue() {
    uint8_t ret = MAX_NRU_VALUE;
    uint8_t nru = getNru();
    if (nru < MAX_NRU_VALUE) {
        ret = ++nru;
        setNru(nru);
    }
    return ret;
}

uint8_t StoredValue::getNRUValue() const {
    return getNru();
}

void StoredValue::setFreqCounterValue(uint16_t newValue) {
    auto taggedPtr = value.get();
    taggedPtr.setTag(newValue);
    value.reset(taggedPtr);
}

uint16_t StoredValue::getFreqCounterValue() const {
    return value.get().getTag();
}

void StoredValue::restoreValue(const Item& itm) {
    if (isTempInitialItem() || isTempDeletedItem()) {
        cas = itm.getCas();
        flags = itm.getFlags();
        exptime = itm.getExptime();
        revSeqno = itm.getRevSeqno();
        bySeqno = itm.getBySeqno();
        setNru(INITIAL_NRU_VALUE);
    }
    datatype = itm.getDataType();
    setDeletedPriv(itm.isDeleted());
    value = itm.getValue();
    setFreqCounterValue(ItemEviction::initialFreqCount);
    setResident(true);
}

void StoredValue::restoreMeta(const Item& itm) {
    cas = itm.getCas();
    flags = itm.getFlags();
    datatype = itm.getDataType();
    exptime = itm.getExptime();
    revSeqno = itm.getRevSeqno();
    if (itm.isDeleted()) {
        setTempDeleted();
    } else { /* Regular item with the full eviction */
        bySeqno = itm.getBySeqno();
        /* set it back to false as we created a temp item by setting it to true
           when bg fetch is scheduled (full eviction mode). */
        setNewCacheItem(false);
    }
    if (getNru() == MAX_NRU_VALUE) {
        setNru(INITIAL_NRU_VALUE);
    }
    setFreqCounterValue(ItemEviction::initialFreqCount);
}

bool StoredValue::del() {
    if (isOrdered()) {
        return static_cast<OrderedStoredValue*>(this)->deleteImpl();
    } else {
        return this->deleteImpl();
    }
}

size_t StoredValue::getRequiredStorage(const Item& item) {
    return sizeof(StoredValue) +
           SerialisedDocKey::getObjectSize(item.getKey().size());
}

std::unique_ptr<Item> StoredValue::toItem(bool lck, uint16_t vbucket) const {
    auto itm =
            std::make_unique<Item>(getKey(),
                                   getFlags(),
                                   getExptime(),
                                   value,
                                   datatype,
                                   lck ? static_cast<uint64_t>(-1) : getCas(),
                                   bySeqno,
                                   vbucket,
                                   getRevSeqno());

    itm->setNRUValue(getNru());

    if (isDeleted()) {
        itm->setDeleted();
    }

    return itm;
}

std::unique_ptr<Item> StoredValue::toItemKeyOnly(uint16_t vbucket) const {
    auto itm =
            std::make_unique<Item>(getKey(),
                                   getFlags(),
                                   getExptime(),
                                   value_t{},
                                   datatype,
                                   getCas(),
                                   getBySeqno(),
                                   vbucket,
                                   getRevSeqno());

    itm->setNRUValue(getNru());

    if (isDeleted()) {
        itm->setDeleted();
    }

    return itm;
}

void StoredValue::reallocate() {
    // Allocate a new Blob for this stored value; copy the existing Blob to
    // the new one and free the old.
    value_t new_val(Blob::Copy(*value));
    resetValueWithCopy(new_val);
}

void StoredValue::Deleter::operator()(StoredValue* val) {
    if (val->isOrdered()) {
        delete static_cast<OrderedStoredValue*>(val);
    } else {
        delete val;
    }
}

OrderedStoredValue* StoredValue::toOrderedStoredValue() {
    if (isOrdered()) {
        return static_cast<OrderedStoredValue*>(this);
    }
    throw std::bad_cast();
}

const OrderedStoredValue* StoredValue::toOrderedStoredValue() const {
    if (isOrdered()) {
        return static_cast<const OrderedStoredValue*>(this);
    }
    throw std::bad_cast();
}

bool StoredValue::operator==(const StoredValue& other) const {
    return (cas == other.cas && revSeqno == other.revSeqno &&
            bySeqno == other.bySeqno &&
            lock_expiry_or_delete_time == other.lock_expiry_or_delete_time &&
            exptime == other.exptime && flags == other.flags &&
            isDirty() == other.isDirty() && isDeleted() == other.isDeleted() &&
            isNewCacheItem() == other.isNewCacheItem() &&
            isOrdered() == other.isOrdered() && getNru() == other.getNru() &&
            isResident() == other.isResident() && getKey() == other.getKey());
}

bool StoredValue::deleteImpl() {
    if (isDeleted() && !getValue()) {
        // SV is already marked as deleted and has no value - no further
        // deletion possible.
        return false;
    }

    resetValue();
    setDatatype(PROTOCOL_BINARY_RAW_BYTES);
    setPendingSeqno();

    setDeletedPriv(true);
    markDirty();

    return true;
}

void StoredValue::setValueImpl(const Item& itm) {
    if (isDeleted() && !itm.isDeleted()) {
        // Transitioning from deleted -> alive - this should be considered
        // a new cache item as it is increasing the number of (alive) items
        // in the vBucket.
        setNewCacheItem(true);
    }

    setDeletedPriv(itm.isDeleted());
    flags = itm.getFlags();
    datatype = itm.getDataType();
    bySeqno = itm.getBySeqno();

    cas = itm.getCas();
    lock_expiry_or_delete_time = 0;
    exptime = itm.getExptime();
    revSeqno = itm.getRevSeqno();

    if (isTempInitialItem()) {
        markClean();
    } else {
        markDirty();
    }

    if (isTempItem()) {
        setResident(false);
    } else {
        setResident(true);
        value = itm.getValue();
    }
}

bool StoredValue::compressValue() {
    if (!mcbp::datatype::is_snappy(datatype)) {
        // Attempt compression only if datatype indicates
        // that the value is not compressed already
        cb::compression::Buffer deflated;
        if (cb::compression::deflate(cb::compression::Algorithm::Snappy,
                                     {value->getData(), value->valueSize()},
                                     deflated)) {
            if (deflated.size() > value->valueSize()) {
                // No point of keeping it compressed if the deflated length
                // is greater than the original length
                return true;
            }
            Blob* data = nullptr;
            data = Blob::New(deflated.data(), deflated.size());
            datatype |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
            resetwithCompressedValue(TaggedPtr<Blob>(data));
        } else {
            return false;
        }
    }

    return true;
}

/**
 * Get an item_info from the StoredValue
 */
boost::optional<item_info> StoredValue::getItemInfo(uint64_t vbuuid) const {
    if (isTempItem()) {
        return boost::none;
    }

    item_info info;
    info.cas = cas;
    info.vbucket_uuid = vbuuid;
    info.seqno = bySeqno;
    info.exptime = exptime;
    info.nbytes = 0;
    info.flags = flags;
    info.datatype = datatype;
    info.document_state =
            isDeleted() ? DocumentState::Deleted : DocumentState::Alive;
    info.nkey = getKey().size();
    info.key = getKey().data();
    if (getValue()) {
        info.value[0].iov_base = const_cast<char*>(getValue()->getData());
        info.value[0].iov_len = getValue()->valueSize();
    }
    return info;
}

std::ostream& operator<<(std::ostream& os, const StoredValue& sv) {

    // type, address
    os << (sv.isOrdered() ? "OSV @" : " SV @") << &sv << " ";

    // datatype: XCJ
    os << (mcbp::datatype::is_xattr(sv.getDatatype()) ? 'X' : '.');
    os << (mcbp::datatype::is_snappy(sv.getDatatype()) ? 'C' : '.');
    os << (mcbp::datatype::is_json(sv.getDatatype()) ? 'J' : '.');
    os << ' ';

    // dirty (Written), deleted, new, locked
    os << (sv.isDirty() ? 'W' : '.');
    os << (sv.isDeleted() ? 'D' : '.');
    os << (sv.isNewCacheItem() ? 'N' : '.');
    os << (sv.isResident() ? 'R' : '.');
    os << (sv.isLocked(ep_current_time()) ? 'L' : '.');
    if (sv.isOrdered()) {
        const auto* osv = sv.toOrderedStoredValue();
        os << (osv->isStalePriv() ? 'S' : '.');
    }
    os << ' ';

    // Temporary states
    os << "temp:"
       << (sv.isTempInitialItem() ? 'I' : ' ')
       << (sv.isTempDeletedItem() ? 'D' : ' ')
       << (sv.isTempNonExistentItem() ? 'N' : ' ')
       << ' ';

    // seqno, revid, expiry / purge time
    os << "seq:" << sv.getBySeqno() << " rev:" << sv.getRevSeqno();
    os << " key:\"" << sv.getKey() << "\"";
    if (sv.isOrdered() && sv.isDeleted()) {
        os << " del_time:" << sv.lock_expiry_or_delete_time;
    } else {
        os << " exp:" << sv.getExptime();
    }

    os << " vallen:" << sv.valuelen();
    if (sv.getValue().get()) {
        os << " val:\"";
        const char* data = sv.getValue()->getData();
        // print up to first 40 bytes of value.
        const size_t limit = std::min(size_t(40), sv.getValue()->valueSize());
        for (size_t ii = 0; ii < limit; ii++) {
            os << data[ii];
        }
        if (limit < sv.getValue()->valueSize()) {
            os << " <cut>";
        }
        os << "\"";
    }
    return os;
}

bool OrderedStoredValue::operator==(const OrderedStoredValue& other) const {
    return StoredValue::operator==(other);
}

size_t OrderedStoredValue::getRequiredStorage(const Item& item) {
    return sizeof(OrderedStoredValue) +
           SerialisedDocKey::getObjectSize(item.getKey());
}

/**
 * Return the time the item was deleted. Only valid for deleted items.
 */
rel_time_t OrderedStoredValue::getDeletedTime() const {
    if (isDeleted()) {
        return lock_expiry_or_delete_time;
    } else {
        throw std::logic_error(
                "OrderedStoredValue::getDeletedItem: Called on Alive item");
    }
}

bool OrderedStoredValue::deleteImpl() {
    if (StoredValue::deleteImpl()) {
        // Need to record the time when an item is deleted for subsequent
        //purging (ephemeral_metadata_purge_age).
        setDeletedTime(ep_current_time());
        return true;
    }
    return false;
}

void OrderedStoredValue::setValueImpl(const Item& itm) {
    StoredValue::setValueImpl(itm);

    // Update the deleted time (note - even if it was already deleted we should
    // refresh this).
    if (isDeleted()) {
        setDeletedTime(ep_current_time());
    }
}

void OrderedStoredValue::setDeletedTime(rel_time_t time) {
    if (!isDeleted()) {
        throw std::logic_error(
                "OrderedStoredValue::setDeletedTime: Called on Alive item");
    }
    lock_expiry_or_delete_time = time;
}
