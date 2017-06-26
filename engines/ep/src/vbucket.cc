/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include <functional>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "atomic.h"
#include "bgfetcher.h"
#include "conflict_resolution.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ep_types.h"
#include "failover-table.h"
#include "flusher.h"
#include "pre_link_document_context.h"
#include "vbucketdeletiontask.h"

#define STATWRITER_NAMESPACE vbucket
#include "statwriter.h"
#undef STATWRITER_NAMESPACE
#include "stored_value_factories.h"

#include "vbucket.h"

#include <xattr/blob.h>
#include <xattr/utils.h>

/* Macros */
const size_t MIN_CHK_FLUSH_TIMEOUT = 10; // 10 sec.
const size_t MAX_CHK_FLUSH_TIMEOUT = 30; // 30 sec.

/* Statics definitions */
std::atomic<size_t> VBucket::chkFlushTimeout(MIN_CHK_FLUSH_TIMEOUT);

VBucketFilter VBucketFilter::filter_diff(const VBucketFilter &other) const {
    std::vector<uint16_t> tmp(acceptable.size() + other.size());
    std::vector<uint16_t>::iterator end;
    end = std::set_symmetric_difference(acceptable.begin(),
                                        acceptable.end(),
                                        other.acceptable.begin(),
                                        other.acceptable.end(),
                                        tmp.begin());
    return VBucketFilter(std::vector<uint16_t>(tmp.begin(), end));
}

VBucketFilter VBucketFilter::filter_intersection(const VBucketFilter &other)
                                                                        const {
    std::vector<uint16_t> tmp(acceptable.size() + other.size());
    std::vector<uint16_t>::iterator end;

    end = std::set_intersection(acceptable.begin(), acceptable.end(),
                                other.acceptable.begin(),
                                other.acceptable.end(),
                                tmp.begin());
    return VBucketFilter(std::vector<uint16_t>(tmp.begin(), end));
}

static bool isRange(std::set<uint16_t>::const_iterator it,
                    const std::set<uint16_t>::const_iterator &end,
                    size_t &length)
{
    length = 0;
    for (uint16_t val = *it;
         it != end && (val + length) == *it;
         ++it, ++length) {
        // empty
    }

    --length;

    return length > 1;
}

std::ostream& operator <<(std::ostream &out, const VBucketFilter &filter)
{
    std::set<uint16_t>::const_iterator it;

    if (filter.acceptable.empty()) {
        out << "{ empty }";
    } else {
        bool needcomma = false;
        out << "{ ";
        for (it = filter.acceptable.begin();
             it != filter.acceptable.end();
             ++it) {
            if (needcomma) {
                out << ", ";
            }

            size_t length;
            if (isRange(it, filter.acceptable.end(), length)) {
                std::set<uint16_t>::iterator last = it;
                for (size_t i = 0; i < length; ++i) {
                    ++last;
                }
                out << "[" << *it << "," << *last << "]";
                it = last;
            } else {
                out << *it;
            }
            needcomma = true;
        }
        out << " }";
    }

    return out;
}

const vbucket_state_t VBucket::ACTIVE =
                     static_cast<vbucket_state_t>(htonl(vbucket_state_active));
const vbucket_state_t VBucket::REPLICA =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_replica));
const vbucket_state_t VBucket::PENDING =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_pending));
const vbucket_state_t VBucket::DEAD =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_dead));

VBucket::VBucket(id_type i,
                 vbucket_state_t newState,
                 EPStats& st,
                 CheckpointConfig& chkConfig,
                 int64_t lastSeqno,
                 uint64_t lastSnapStart,
                 uint64_t lastSnapEnd,
                 std::unique_ptr<FailoverTable> table,
                 std::shared_ptr<Callback<id_type> > flusherCb,
                 std::unique_ptr<AbstractStoredValueFactory> valFact,
                 NewSeqnoCallback newSeqnoCb,
                 Configuration& config,
                 item_eviction_policy_t evictionPolicy,
                 vbucket_state_t initState,
                 uint64_t purgeSeqno,
                 uint64_t maxCas,
                 int64_t hlcEpochSeqno,
                 const std::string& collectionsManifest)
    : ht(st, std::move(valFact), config.getHtSize(), config.getHtLocks()),
      checkpointManager(st,
                        i,
                        chkConfig,
                        lastSeqno,
                        lastSnapStart,
                        lastSnapEnd,
                        flusherCb),
      failovers(std::move(table)),
      opsCreate(0),
      opsUpdate(0),
      opsDelete(0),
      opsReject(0),
      dirtyQueueSize(0),
      dirtyQueueMem(0),
      dirtyQueueFill(0),
      dirtyQueueDrain(0),
      dirtyQueueAge(0),
      dirtyQueuePendingWrites(0),
      metaDataDisk(0),
      numExpiredItems(0),
      eviction(evictionPolicy),
      stats(st),
      persistenceSeqno(0),
      numHpVBReqs(0),
      id(i),
      state(newState),
      initialState(initState),
      purge_seqno(purgeSeqno),
      takeover_backed_up(false),
      persisted_snapshot_start(lastSnapStart),
      persisted_snapshot_end(lastSnapEnd),
      rollbackItemCount(0),
      hlc(maxCas,
          hlcEpochSeqno,
          std::chrono::microseconds(config.getHlcDriftAheadThresholdUs()),
          std::chrono::microseconds(config.getHlcDriftBehindThresholdUs())),
      statPrefix("vb_" + std::to_string(i)),
      persistenceCheckpointId(0),
      bucketCreation(false),
      deferredDeletion(false),
      deferredDeletionCookie(nullptr),
      newSeqnoCb(std::move(newSeqnoCb)),
      manifest(collectionsManifest) {
    if (config.getConflictResolutionType().compare("lww") == 0) {
        conflictResolver.reset(new LastWriteWinsResolution());
    } else {
        conflictResolver.reset(new RevisionSeqnoResolution());
    }

    backfill.isBackfillPhase = false;
    pendingOpsStart = 0;
    stats.memOverhead->fetch_add(sizeof(VBucket)
                                + ht.memorySize() + sizeof(CheckpointManager));
    LOG(EXTENSION_LOG_NOTICE,
        "VBucket: created vbucket:%" PRIu16
        " with state:%s "
        "initialState:%s lastSeqno:%" PRIu64 " lastSnapshot:{%" PRIu64
        ",%" PRIu64 "} persisted_snapshot:{%" PRIu64 ",%" PRIu64
        "} max_cas:%" PRIu64 " uuid:%s",
        id,
        VBucket::toString(state),
        VBucket::toString(initialState),
        lastSeqno,
        lastSnapStart,
        lastSnapEnd,
        persisted_snapshot_start,
        persisted_snapshot_end,
        getMaxCas(),
        failovers ? std::to_string(failovers->getLatestUUID()).c_str() : "<>");
}

VBucket::~VBucket() {
    if (!pendingOps.empty()) {
        LOG(EXTENSION_LOG_WARNING,
            "~Vbucket(): vbucket:%" PRIu16 " has %ld pending ops",
            id,
            pendingOps.size());
    }

    stats.diskQueueSize.fetch_sub(dirtyQueueSize.load());
    stats.vbBackfillQueueSize.fetch_sub(getBackfillSize());

    // Clear out the bloomfilter(s)
    clearFilter();

    stats.memOverhead->fetch_sub(sizeof(VBucket) + ht.memorySize() +
                                sizeof(CheckpointManager));

    LOG(EXTENSION_LOG_INFO, "Destroying vbucket %d\n", id);
}

void VBucket::fireAllOps(EventuallyPersistentEngine &engine,
                         ENGINE_ERROR_CODE code) {
    std::unique_lock<std::mutex> lh(pendingOpLock);

    if (pendingOpsStart > 0) {
        hrtime_t now = gethrtime();
        if (now > pendingOpsStart) {
            hrtime_t d = (now - pendingOpsStart) / 1000;
            stats.pendingOpsHisto.add(d);
            atomic_setIfBigger(stats.pendingOpsMaxDuration, d);
        }
    } else {
        return;
    }

    pendingOpsStart = 0;
    stats.pendingOps.fetch_sub(pendingOps.size());
    atomic_setIfBigger(stats.pendingOpsMax, pendingOps.size());

    while (!pendingOps.empty()) {
        const void *pendingOperation = pendingOps.back();
        pendingOps.pop_back();
        // We don't want to hold the pendingOpLock when
        // calling notifyIOComplete.
        lh.unlock();
        engine.notifyIOComplete(pendingOperation, code);
        lh.lock();
    }

    LOG(EXTENSION_LOG_INFO,
        "Fired pendings ops for vbucket %" PRIu16 " in state %s\n",
        id, VBucket::toString(state));
}

void VBucket::fireAllOps(EventuallyPersistentEngine &engine) {

    if (state == vbucket_state_active) {
        fireAllOps(engine, ENGINE_SUCCESS);
    } else if (state == vbucket_state_pending) {
        // Nothing
    } else {
        fireAllOps(engine, ENGINE_NOT_MY_VBUCKET);
    }
}

void VBucket::setState(vbucket_state_t to) {
    WriterLockHolder wlh(stateLock);
    setState_UNLOCKED(to, wlh);
}

void VBucket::setState_UNLOCKED(vbucket_state_t to,
                                WriterLockHolder &vbStateLock) {
    vbucket_state_t oldstate = state;

    if (to == vbucket_state_active &&
        checkpointManager.getOpenCheckpointId() < 2) {
        checkpointManager.setOpenCheckpointId(2);
    }

    LOG(EXTENSION_LOG_NOTICE,
        "VBucket::setState: transitioning vbucket:%" PRIu16 " from:%s to:%s",
        id,
        VBucket::toString(oldstate),
        VBucket::toString(to));
    
    state = to;
}

vbucket_state VBucket::getVBucketState() const {
     auto persisted_range = getPersistedSnapshot();

     return vbucket_state{getState(),
                          getPersistenceCheckpointId(),
                          0,
                          getHighSeqno(),
                          getPurgeSeqno(),
                          persisted_range.start,
                          persisted_range.end,
                          getMaxCas(),
                          hlc.getEpochSeqno(),
                          failovers->toJSON()};
}



void VBucket::doStatsForQueueing(const Item& qi, size_t itemBytes)
{
    ++dirtyQueueSize;
    dirtyQueueMem.fetch_add(sizeof(Item));
    ++dirtyQueueFill;
    dirtyQueueAge.fetch_add(qi.getQueuedTime());
    dirtyQueuePendingWrites.fetch_add(itemBytes);
}

void VBucket::doStatsForFlushing(const Item& qi, size_t itemBytes) {
    --dirtyQueueSize;
    decrDirtyQueueMem(sizeof(Item));
    ++dirtyQueueDrain;
    decrDirtyQueueAge(qi.getQueuedTime());
    decrDirtyQueuePendingWrites(itemBytes);
}

void VBucket::incrMetaDataDisk(const Item& qi) {
    metaDataDisk.fetch_add(qi.getKey().size() + sizeof(ItemMetaData));
}

void VBucket::decrMetaDataDisk(const Item& qi) {
    // assume couchstore remove approx this much data from disk
    metaDataDisk.fetch_sub((qi.getKey().size() + sizeof(ItemMetaData)));
}

void VBucket::resetStats() {
    opsCreate.store(0);
    opsUpdate.store(0);
    opsDelete.store(0);
    opsReject.store(0);

    stats.diskQueueSize.fetch_sub(dirtyQueueSize.exchange(0));
    dirtyQueueMem.store(0);
    dirtyQueueFill.store(0);
    dirtyQueueAge.store(0);
    dirtyQueuePendingWrites.store(0);
    dirtyQueueDrain.store(0);

    hlc.resetStats();
}

uint64_t VBucket::getQueueAge() {
    uint64_t currDirtyQueueAge = dirtyQueueAge.load(std::memory_order_relaxed);
    rel_time_t currentAge = ep_current_time() * dirtyQueueSize;
    if (currentAge < currDirtyQueueAge) {
        return 0;
    }
    return (currentAge - currDirtyQueueAge) * 1000;
}

template <typename T>
void VBucket::addStat(const char *nm, const T &val, ADD_STAT add_stat,
                      const void *c) {
    std::string stat = statPrefix;
    if (nm != NULL) {
        add_prefixed_stat(statPrefix, nm, val, add_stat, c);
    } else {
        add_casted_stat(statPrefix.data(), val, add_stat, c);
    }
}

void VBucket::handlePreExpiry(StoredValue& v) {
    value_t value = v.getValue();
    if (value) {
        std::unique_ptr<Item> itm(v.toItem(false, id));
        item_info itm_info;
        EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();
        itm_info =
                itm->toItemInfo(failovers->getLatestUUID(), getHLCEpochSeqno());
        value_t new_val(Blob::Copy(*value));
        itm->setValue(new_val);

        SERVER_HANDLE_V1* sapi = engine->getServerApi();
        /* TODO: In order to minimize allocations, the callback needs to
         * allocate an item whose value size will be exactly the size of the
         * value after pre-expiry is performed.
         */
        if (sapi->document->pre_expiry(itm_info)) {
            char* extMeta = const_cast<char *>(v.getValue()->getExtMeta());
            Item new_item(v.getKey(), v.getFlags(), v.getExptime(),
                          itm_info.value[0].iov_base, itm_info.value[0].iov_len,
                          reinterpret_cast<uint8_t*>(extMeta),
                          v.getValue()->getExtLen(), v.getCas(),
                          v.getBySeqno(), id, v.getRevSeqno(),
                          v.getNRUValue());

            new_item.setDeleted();
            ht.setValue(new_item, v);
        }
    }
}

size_t VBucket::getNumNonResidentItems() const {
    if (eviction == VALUE_ONLY) {
        return ht.getNumInMemoryNonResItems();
    } else {
        size_t num_items = ht.getNumItems();
        size_t num_res_items = ht.getNumInMemoryItems() -
                               ht.getNumInMemoryNonResItems();
        return num_items > num_res_items ? (num_items - num_res_items) : 0;
    }
}


uint64_t VBucket::getPersistenceCheckpointId() const {
    return persistenceCheckpointId.load();
}

void VBucket::setPersistenceCheckpointId(uint64_t checkpointId) {
    persistenceCheckpointId.store(checkpointId);
}

void VBucket::markDirty(const DocKey& key) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = ht.unlocked_find(
            key, hbl.getBucketNum(), WantsDeleted::Yes, TrackReference::Yes);
    if (v) {
        v->markDirty();
    } else {
        LOG(EXTENSION_LOG_WARNING, "markDirty: Error marking dirty, a key "
            "is missing from vb:%" PRIu16, id);
    }
}

bool VBucket::isResidentRatioUnderThreshold(float threshold) {
    if (eviction != FULL_EVICTION) {
        throw std::invalid_argument("VBucket::isResidentRatioUnderThreshold: "
                "policy (which is " + std::to_string(eviction) +
                ") must be FULL_EVICTION");
    }
    size_t num_items = getNumItems();
    size_t num_non_resident_items = getNumNonResidentItems();
    if (threshold >= ((float)(num_items - num_non_resident_items) /
                                                                num_items)) {
        return true;
    } else {
        return false;
    }
}

void VBucket::createFilter(size_t key_count, double probability) {
    // Create the actual bloom filter upon vbucket creation during
    // scenarios:
    //      - Bucket creation
    //      - Rebalance
    LockHolder lh(bfMutex);
    if (bFilter == nullptr && tempFilter == nullptr) {
        bFilter = std::make_unique<BloomFilter>(key_count, probability,
                                        BFILTER_ENABLED);
    } else {
        LOG(EXTENSION_LOG_WARNING, "(vb %" PRIu16 ") Bloom filter / Temp filter"
            " already exist!", id);
    }
}

void VBucket::initTempFilter(size_t key_count, double probability) {
    // Create a temp bloom filter with status as COMPACTING,
    // if the main filter is found to exist, set its state to
    // COMPACTING as well.
    LockHolder lh(bfMutex);
    tempFilter = std::make_unique<BloomFilter>(key_count, probability,
                                     BFILTER_COMPACTING);
    if (bFilter) {
        bFilter->setStatus(BFILTER_COMPACTING);
    }
}

void VBucket::addToFilter(const DocKey& key) {
    LockHolder lh(bfMutex);
    if (bFilter) {
        bFilter->addKey(key);
    }

    // If the temp bloom filter is not found to be NULL,
    // it means that compaction is running on the particular
    // vbucket. Therefore add the key to the temp filter as
    // well, as once compaction completes the temp filter
    // will replace the main bloom filter.
    if (tempFilter) {
        tempFilter->addKey(key);
    }
}

bool VBucket::maybeKeyExistsInFilter(const DocKey& key) {
    LockHolder lh(bfMutex);
    if (bFilter) {
        return bFilter->maybeKeyExists(key);
    } else {
        // If filter doesn't exist, allow the BgFetch to go through.
        return true;
    }
}

bool VBucket::isTempFilterAvailable() {
    LockHolder lh(bfMutex);
    if (tempFilter &&
        (tempFilter->getStatus() == BFILTER_COMPACTING ||
         tempFilter->getStatus() == BFILTER_ENABLED)) {
        return true;
    } else {
        return false;
    }
}

void VBucket::addToTempFilter(const DocKey& key) {
    // Keys will be added to only the temp filter during
    // compaction.
    LockHolder lh(bfMutex);
    if (tempFilter) {
        tempFilter->addKey(key);
    }
}

void VBucket::swapFilter() {
    // Delete the main bloom filter and replace it with
    // the temp filter that was populated during compaction,
    // only if the temp filter's state is found to be either at
    // COMPACTING or ENABLED (if in the case the user enables
    // bloomfilters for some reason while compaction was running).
    // Otherwise, it indicates that the filter's state was
    // possibly disabled during compaction, therefore clear out
    // the temp filter. If it gets enabled at some point, a new
    // bloom filter will be made available after the next
    // compaction.

    LockHolder lh(bfMutex);
    if (tempFilter) {
        bFilter.reset();

        if (tempFilter->getStatus() == BFILTER_COMPACTING ||
             tempFilter->getStatus() == BFILTER_ENABLED) {
            bFilter = std::move(tempFilter);
            bFilter->setStatus(BFILTER_ENABLED);
        }
        tempFilter.reset();
    }
}

void VBucket::clearFilter() {
    LockHolder lh(bfMutex);
    bFilter.reset();
    tempFilter.reset();
}

void VBucket::setFilterStatus(bfilter_status_t to) {
    LockHolder lh(bfMutex);
    if (bFilter) {
        bFilter->setStatus(to);
    }
    if (tempFilter) {
        tempFilter->setStatus(to);
    }
}

std::string VBucket::getFilterStatusString() {
    LockHolder lh(bfMutex);
    if (bFilter) {
        return bFilter->getStatusString();
    } else if (tempFilter) {
        return tempFilter->getStatusString();
    } else {
        return "DOESN'T EXIST";
    }
}

size_t VBucket::getFilterSize() {
    LockHolder lh(bfMutex);
    if (bFilter) {
        return bFilter->getFilterSize();
    } else {
        return 0;
    }
}

size_t VBucket::getNumOfKeysInFilter() {
    LockHolder lh(bfMutex);
    if (bFilter) {
        return bFilter->getNumOfKeysInFilter();
    } else {
        return 0;
    }
}

VBNotifyCtx VBucket::queueDirty(
        StoredValue& v,
        const GenerateBySeqno generateBySeqno,
        const GenerateCas generateCas,
        const bool isBackfillItem,
        PreLinkDocumentContext* preLinkDocumentContext) {
    VBNotifyCtx notifyCtx;

    queued_item qi(v.toItem(false, getId()));

    if (isBackfillItem) {
        queueBackfillItem(qi, generateBySeqno);
        notifyCtx.notifyFlusher = true;
        /* During backfill on a TAP receiver we need to update the snapshot
         range in the checkpoint. Has to be done here because in case of TAP
         backfill, above, we use vb.queueBackfillItem() instead of
         vb.checkpointManager.queueDirty() */
        if (generateBySeqno == GenerateBySeqno::Yes) {
            checkpointManager.resetSnapshotRange();
        }
    } else {
        notifyCtx.notifyFlusher =
                checkpointManager.queueDirty(*this,
                                             qi,
                                             generateBySeqno,
                                             generateCas,
                                             preLinkDocumentContext);
        notifyCtx.notifyReplication = true;
        if (GenerateCas::Yes == generateCas) {
            v.setCas(qi->getCas());
        }
    }

    v.setBySeqno(qi->getBySeqno());
    notifyCtx.bySeqno = qi->getBySeqno();

    return notifyCtx;
}

StoredValue* VBucket::fetchValidValue(HashTable::HashBucketLock& hbl,
                                      const DocKey& key,
                                      WantsDeleted wantsDeleted,
                                      TrackReference trackReference,
                                      QueueExpired queueExpired) {
    if (!hbl.getHTLock()) {
        throw std::logic_error(
                "Hash bucket lock not held in "
                "VBucket::fetchValidValue() for hash bucket: " +
                std::to_string(hbl.getBucketNum()) + "for key: " +
                std::string(reinterpret_cast<const char*>(key.data()),
                            key.size()));
    }
    StoredValue* v = ht.unlocked_find(
            key, hbl.getBucketNum(), wantsDeleted, trackReference);
    if (v && !v->isDeleted() && !v->isTempItem()) {
        // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            if (getState() != vbucket_state_active) {
                return wantsDeleted == WantsDeleted::Yes ? v : NULL;
            }

            // queueDirty only allowed on active VB
            if (queueExpired == QueueExpired::Yes &&
                getState() == vbucket_state_active) {
                incExpirationStat(ExpireBy::Access);
                handlePreExpiry(*v);
                VBNotifyCtx notifyCtx;
                std::tie(std::ignore, v, notifyCtx) =
                        processExpiredItem(hbl, *v);
                notifyNewSeqno(notifyCtx);
            }
            return wantsDeleted == WantsDeleted::Yes ? v : NULL;
        }
    }
    return v;
}

void VBucket::incExpirationStat(const ExpireBy source) {
    switch (source) {
    case ExpireBy::Pager:
        ++stats.expired_pager;
        break;
    case ExpireBy::Compactor:
        ++stats.expired_compactor;
        break;
    case ExpireBy::Access:
        ++stats.expired_access;
        break;
    }
    ++numExpiredItems;
}

MutationStatus VBucket::setFromInternal(Item& itm) {
    return ht.set(itm);
}

ENGINE_ERROR_CODE VBucket::set(Item& itm,
                               const void* cookie,
                               EventuallyPersistentEngine& engine,
                               const int bgFetchDelay,
                               cb::StoreIfPredicate predicate) {
    bool cas_op = (itm.getCas() != 0);
    auto hbl = ht.getLockedBucket(itm.getKey());
    StoredValue* v = ht.unlocked_find(itm.getKey(),
                                      hbl.getBucketNum(),
                                      WantsDeleted::Yes,
                                      TrackReference::No);
    if (v) {
        if (predicate &&
            !predicate(v->getItemInfo(failovers->getLatestUUID()))) {
            return ENGINE_PREDICATE_FAILED;
        }
    }
    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    bool maybeKeyExists = true;
    // If we didn't find a valid item, check Bloomfilter's prediction if in
    // full eviction policy and for a CAS operation.
    if ((v == nullptr || v->isTempInitialItem()) &&
        (eviction == FULL_EVICTION) && (itm.getCas() != 0)) {
        // Check Bloomfilter's prediction
        if (!maybeKeyExistsInFilter(itm.getKey())) {
            maybeKeyExists = false;
        }
    }

    PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
    VBQueueItemCtx queueItmCtx(GenerateBySeqno::Yes,
                               GenerateCas::Yes,
                               TrackCasDrift::No,
                               /*isBackfillItem*/ false,
                               &preLinkDocumentContext);

    MutationStatus status;
    VBNotifyCtx notifyCtx;
    std::tie(status, notifyCtx) = processSet(hbl,
                                             v,
                                             itm,
                                             itm.getCas(),
                                             /*allowExisting*/ true,
                                             /*hashMetaData*/ false,
                                             queueItmCtx,
                                             maybeKeyExists);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (status) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::IsLocked:
        ret = ENGINE_LOCKED;
        break;
    case MutationStatus::NotFound:
        if (cas_op) {
            ret = ENGINE_KEY_ENOENT;
            break;
        }
    // FALLTHROUGH
    case MutationStatus::WasDirty:
    // Even if the item was dirty, push it into the vbucket's open
    // checkpoint.
    case MutationStatus::WasClean:
        notifyNewSeqno(notifyCtx);

        itm.setBySeqno(v->getBySeqno());
        itm.setCas(v->getCas());
        break;
    case MutationStatus::NeedBgFetch: { // CAS operation with non-resident item
        // +
        // full eviction.
        if (v) {
            // temp item is already created. Simply schedule a bg fetch job
            hbl.getHTLock().unlock();
            bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
            return ENGINE_EWOULDBLOCK;
        }
        ret = addTempItemAndBGFetch(
                hbl, itm.getKey(), cookie, engine, bgFetchDelay, true);
        break;
    }
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::replace(Item& itm,
                                   const void* cookie,
                                   EventuallyPersistentEngine& engine,
                                   const int bgFetchDelay,
                                   cb::StoreIfPredicate predicate) {
    auto hbl = ht.getLockedBucket(itm.getKey());
    StoredValue* v = ht.unlocked_find(itm.getKey(),
                                      hbl.getBucketNum(),
                                      WantsDeleted::Yes,
                                      TrackReference::No);
    if (v) {
        if (predicate &&
            !predicate(v->getItemInfo(failovers->getLatestUUID()))) {
            return ENGINE_PREDICATE_FAILED;
        }
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return ENGINE_KEY_ENOENT;
        }

        MutationStatus mtype;
        VBNotifyCtx notifyCtx;
        if (eviction == FULL_EVICTION && v->isTempInitialItem()) {
            mtype = MutationStatus::NeedBgFetch;
        } else {
            PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
            VBQueueItemCtx queueItmCtx(GenerateBySeqno::Yes,
                                       GenerateCas::Yes,
                                       TrackCasDrift::No,
                                       /*isBackfillItem*/ false,
                                       &preLinkDocumentContext);
            std::tie(mtype, notifyCtx) = processSet(hbl,
                                                    v,
                                                    itm,
                                                    0,
                                                    /*allowExisting*/ true,
                                                    /*hasMetaData*/ false,
                                                    queueItmCtx);
        }

        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        switch (mtype) {
        case MutationStatus::NoMem:
            ret = ENGINE_ENOMEM;
            break;
        case MutationStatus::IsLocked:
            ret = ENGINE_LOCKED;
            break;
        case MutationStatus::InvalidCas:
        case MutationStatus::NotFound:
            ret = ENGINE_NOT_STORED;
            break;
        // FALLTHROUGH
        case MutationStatus::WasDirty:
        // Even if the item was dirty, push it into the vbucket's open
        // checkpoint.
        case MutationStatus::WasClean:
            notifyNewSeqno(notifyCtx);

            itm.setBySeqno(v->getBySeqno());
            itm.setCas(v->getCas());
            break;
        case MutationStatus::NeedBgFetch: {
            // temp item is already created. Simply schedule a bg fetch job
            hbl.getHTLock().unlock();
            bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
            ret = ENGINE_EWOULDBLOCK;
            break;
        }
        }

        return ret;
    } else {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        }

        if (maybeKeyExistsInFilter(itm.getKey())) {
            return addTempItemAndBGFetch(
                    hbl, itm.getKey(), cookie, engine, bgFetchDelay, false);
        } else {
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT for replace().
            return ENGINE_KEY_ENOENT;
        }
    }
}

ENGINE_ERROR_CODE VBucket::addBackfillItem(Item& itm,
                                           const GenerateBySeqno genBySeqno) {
    auto hbl = ht.getLockedBucket(itm.getKey());
    StoredValue* v = ht.unlocked_find(itm.getKey(),
                                      hbl.getBucketNum(),
                                      WantsDeleted::Yes,
                                      TrackReference::No);

    // Note that this function is only called on replica or pending vbuckets.
    if (v && v->isLocked(ep_current_time())) {
        v->unlock();
    }

    VBQueueItemCtx queueItmCtx(genBySeqno,
                               GenerateCas::No,
                               TrackCasDrift::No,
                               /*isBackfillItem*/ true,
                               nullptr /* No pre link should happen */);
    MutationStatus status;
    VBNotifyCtx notifyCtx;
    std::tie(status, notifyCtx) = processSet(hbl,
                                             v,
                                             itm,
                                             0,
                                             /*allowExisting*/ true,
                                             /*hasMetaData*/ true,
                                             queueItmCtx);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (status) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
    case MutationStatus::IsLocked:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::WasDirty:
    // FALLTHROUGH, to ensure the bySeqno for the hashTable item is
    // set correctly, and also the sequence numbers are ordered correctly.
    // (MB-14003)
    case MutationStatus::NotFound:
    // FALLTHROUGH
    case MutationStatus::WasClean: {
        setMaxCas(v->getCas());
        // we unlock ht lock here because we want to avoid potential lock
        // inversions arising from notifyNewSeqno() call
        hbl.getHTLock().unlock();
        notifyNewSeqno(notifyCtx);
    } break;
    case MutationStatus::NeedBgFetch:
        throw std::logic_error(
                "VBucket::addBackfillItem: "
                "SET on a non-active vbucket should not require a "
                "bg_metadata_fetch.");
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::setWithMeta(Item& itm,
                                       const uint64_t cas,
                                       uint64_t* seqno,
                                       const void* cookie,
                                       EventuallyPersistentEngine& engine,
                                       const int bgFetchDelay,
                                       const bool force,
                                       const bool allowExisting,
                                       const GenerateBySeqno genBySeqno,
                                       const GenerateCas genCas,
                                       const bool isReplication) {
    auto hbl = ht.getLockedBucket(itm.getKey());
    StoredValue* v = ht.unlocked_find(itm.getKey(),
                                      hbl.getBucketNum(),
                                      WantsDeleted::Yes,
                                      TrackReference::No);

    bool maybeKeyExists = true;
    if (!force) {
        if (v) {
            if (v->isTempInitialItem()) {
                bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
                return ENGINE_EWOULDBLOCK;
            }

            if (!(conflictResolver->resolve(*v,
                                            itm.getMetaData(),
                                            itm.getDataType(),
                                            itm.isDeleted()))) {
                ++stats.numOpsSetMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            if (maybeKeyExistsInFilter(itm.getKey())) {
                return addTempItemAndBGFetch(hbl,
                                             itm.getKey(),
                                             cookie,
                                             engine,
                                             bgFetchDelay,
                                             true,
                                             isReplication);
            } else {
                maybeKeyExists = false;
            }
        }
    } else {
        if (eviction == FULL_EVICTION) {
            // Check Bloomfilter's prediction
            if (!maybeKeyExistsInFilter(itm.getKey())) {
                maybeKeyExists = false;
            }
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    VBQueueItemCtx queueItmCtx(genBySeqno,
                               genCas,
                               TrackCasDrift::Yes,
                               /*isBackfillItem*/ false,
                               nullptr /* No pre link step needed */);
    MutationStatus status;
    VBNotifyCtx notifyCtx;
    std::tie(status, notifyCtx) = processSet(hbl,
                                             v,
                                             itm,
                                             cas,
                                             allowExisting,
                                             true,
                                             queueItmCtx,
                                             maybeKeyExists,
                                             isReplication);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (status) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::IsLocked:
        ret = ENGINE_LOCKED;
        break;
    case MutationStatus::WasDirty:
    case MutationStatus::WasClean: {
        if (seqno) {
            *seqno = static_cast<uint64_t>(v->getBySeqno());
        }
        // we unlock ht lock here because we want to avoid potential lock
        // inversions arising from notifyNewSeqno() call
        hbl.getHTLock().unlock();
        notifyNewSeqno(notifyCtx);
    } break;
    case MutationStatus::NotFound:
        ret = ENGINE_KEY_ENOENT;
        break;
    case MutationStatus::NeedBgFetch: { // CAS operation with non-resident item
        // + full eviction.
        if (v) { // temp item is already created. Simply schedule a
            hbl.getHTLock().unlock(); // bg fetch job.
            bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
            return ENGINE_EWOULDBLOCK;
        }
        ret = addTempItemAndBGFetch(hbl,
                                    itm.getKey(),
                                    cookie,
                                    engine,
                                    bgFetchDelay,
                                    true,
                                    isReplication);
    }
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::deleteItem(const DocKey& key,
                                      uint64_t& cas,
                                      const void* cookie,
                                      EventuallyPersistentEngine& engine,
                                      const int bgFetchDelay,
                                      ItemMetaData* itemMeta,
                                      mutation_descr_t* mutInfo) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = ht.unlocked_find(
            key, hbl.getBucketNum(), WantsDeleted::Yes, TrackReference::No);

    if (!v || v->isDeleted() || v->isTempItem()) {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else { // Full eviction.
            if (!v) { // Item might be evicted from cache.
                if (maybeKeyExistsInFilter(key)) {
                    return addTempItemAndBGFetch(
                            hbl, key, cookie, engine, bgFetchDelay, true);
                } else {
                    // As bloomfilter predicted that item surely doesn't
                    // exist on disk, return ENOENT for deleteItem().
                    return ENGINE_KEY_ENOENT;
                }
            } else if (v->isTempInitialItem()) {
                hbl.getHTLock().unlock();
                bgFetch(key, cookie, engine, bgFetchDelay, true);
                return ENGINE_EWOULDBLOCK;
            } else { // Non-existent or deleted key.
                if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
                    // Delete a temp non-existent item to ensure that
                    // if a delete were issued over an item that doesn't
                    // exist, then we don't preserve a temp item.
                    deleteStoredValue(hbl, *v);
                }
                return ENGINE_KEY_ENOENT;
            }
        }
    }

    if (v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    if (itemMeta != nullptr) {
        itemMeta->cas = v->getCas();
    }

    MutationStatus delrv;
    VBNotifyCtx notifyCtx;
    if (v->isExpired(ep_real_time())) {
        std::tie(delrv, v, notifyCtx) = processExpiredItem(hbl, *v);
    } else {
        ItemMetaData metadata;
        metadata.revSeqno = v->getRevSeqno() + 1;
        std::tie(delrv, v, notifyCtx) =
                processSoftDelete(hbl,
                                  *v,
                                  cas,
                                  metadata,
                                  VBQueueItemCtx(GenerateBySeqno::Yes,
                                                 GenerateCas::Yes,
                                                 TrackCasDrift::No,
                                                 /*isBackfillItem*/ false,
                                                 nullptr /* no pre link */),
                                  /*use_meta*/ false,
                                  /*bySeqno*/ v->getBySeqno());
    }

    uint64_t seqno = 0;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (delrv) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::IsLocked:
        ret = ENGINE_LOCKED_TMPFAIL;
        break;
    case MutationStatus::NotFound:
        ret = ENGINE_KEY_ENOENT;
    /* Fallthrough:
     * A NotFound return value at this point indicates that the
     * item has expired. But, a deletion still needs to be queued
     * for the item in order to persist it.
     */
    case MutationStatus::WasClean:
    case MutationStatus::WasDirty:
        if (itemMeta != nullptr) {
            itemMeta->revSeqno = v->getRevSeqno();
            itemMeta->flags = v->getFlags();
            itemMeta->exptime = v->getExptime();
        }

        notifyNewSeqno(notifyCtx);
        seqno = static_cast<uint64_t>(v->getBySeqno());
        cas = v->getCas();

        if (delrv != MutationStatus::NotFound) {
            if (mutInfo) {
                mutInfo->seqno = seqno;
                mutInfo->vbucket_uuid = failovers->getLatestUUID();
            }
            if (itemMeta != nullptr) {
                itemMeta->cas = v->getCas();
            }
        }
        break;
    case MutationStatus::NeedBgFetch:
        // We already figured out if a bg fetch is requred for a full-evicted
        // item above.
        throw std::logic_error(
                "VBucket::deleteItem: "
                "Unexpected NEEDS_BG_FETCH from processSoftDelete");
    }
    return ret;
}

ENGINE_ERROR_CODE VBucket::deleteWithMeta(const DocKey& key,
                                          uint64_t& cas,
                                          uint64_t* seqno,
                                          const void* cookie,
                                          EventuallyPersistentEngine& engine,
                                          const int bgFetchDelay,
                                          const bool force,
                                          const ItemMetaData& itemMeta,
                                          const bool backfill,
                                          const GenerateBySeqno genBySeqno,
                                          const GenerateCas generateCas,
                                          const uint64_t bySeqno,
                                          const bool isReplication) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = ht.unlocked_find(
            key, hbl.getBucketNum(), WantsDeleted::Yes, TrackReference::No);
    if (!force) { // Need conflict resolution.
        if (v) {
            if (v->isTempInitialItem()) {
                bgFetch(key, cookie, engine, bgFetchDelay, true);
                return ENGINE_EWOULDBLOCK;
            }

            if (!(conflictResolver->resolve(*v,
                                            itemMeta,
                                            PROTOCOL_BINARY_RAW_BYTES,
                                            true))) {
                ++stats.numOpsDelMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            // Item is 1) deleted or not existent in the value eviction case OR
            // 2) deleted or evicted in the full eviction.
            if (maybeKeyExistsInFilter(key)) {
                return addTempItemAndBGFetch(hbl,
                                             key,
                                             cookie,
                                             engine,
                                             bgFetchDelay,
                                             true,
                                             isReplication);
            } else {
                // Even though bloomfilter predicted that item doesn't exist
                // on disk, we must put this delete on disk if the cas is valid.
                AddStatus rv = addTempStoredValue(hbl, key, isReplication);
                if (rv == AddStatus::NoMem) {
                    return ENGINE_ENOMEM;
                }
                v = ht.unlocked_find(key,
                                     hbl.getBucketNum(),
                                     WantsDeleted::Yes,
                                     TrackReference::No);
                v->setDeleted();
            }
        }
    } else {
        if (!v) {
            // We should always try to persist a delete here.
            AddStatus rv = addTempStoredValue(hbl, key, isReplication);
            if (rv == AddStatus::NoMem) {
                return ENGINE_ENOMEM;
            }
            v = ht.unlocked_find(key,
                                 hbl.getBucketNum(),
                                 WantsDeleted::Yes,
                                 TrackReference::No);
            v->setDeleted();
            v->setCas(cas);
        } else if (v->isTempInitialItem()) {
            v->setDeleted();
            v->setCas(cas);
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    MutationStatus delrv;
    VBNotifyCtx notifyCtx;
    if (!v) {
        if (eviction == FULL_EVICTION) {
            delrv = MutationStatus::NeedBgFetch;
        } else {
            delrv = MutationStatus::NotFound;
        }
    } else {
        VBQueueItemCtx queueItmCtx(genBySeqno,
                                   generateCas,
                                   TrackCasDrift::Yes,
                                   backfill,
                                   nullptr /* No pre link step needed */);

        // system xattrs must remain
        std::unique_ptr<Item> itm;
        if (mcbp::datatype::is_xattr(v->getDatatype()) &&
            (itm = pruneXattrDocument(*v, itemMeta))) {
            std::tie(v, delrv, notifyCtx) =
                    updateStoredValue(hbl, *v, *itm, queueItmCtx);
        } else {
            std::tie(delrv, v, notifyCtx) = processSoftDelete(hbl,
                                                              *v,
                                                              cas,
                                                              itemMeta,
                                                              queueItmCtx,
                                                              /*use_meta*/ true,
                                                              bySeqno);
        }
    }
    cas = v ? v->getCas() : 0;

    switch (delrv) {
    case MutationStatus::NoMem:
        return ENGINE_ENOMEM;
    case MutationStatus::InvalidCas:
        return ENGINE_KEY_EEXISTS;
    case MutationStatus::IsLocked:
        return ENGINE_LOCKED_TMPFAIL;
    case MutationStatus::NotFound:
        return ENGINE_KEY_ENOENT;
    case MutationStatus::WasDirty:
    case MutationStatus::WasClean: {
        if (seqno) {
            *seqno = static_cast<uint64_t>(v->getBySeqno());
        }
        // we unlock ht lock here because we want to avoid potential lock
        // inversions arising from notifyNewSeqno() call
        hbl.getHTLock().unlock();
        notifyNewSeqno(notifyCtx);
        break;
    }
    case MutationStatus::NeedBgFetch:
        hbl.getHTLock().unlock();
        bgFetch(key, cookie, engine, bgFetchDelay, true);
        return ENGINE_EWOULDBLOCK;
    }
    return ENGINE_SUCCESS;
}

void VBucket::deleteExpiredItem(const Item& it,
                                time_t startTime,
                                ExpireBy source) {

    // The item is correctly trimmed (by the caller). Fetch the one in the
    // hashtable and replace it if the CAS match (same item; no race).
    // If not found in the hashtable we should add it as a deleted item
    const DocKey& key = it.getKey();
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = ht.unlocked_find(
            key, hbl.getBucketNum(), WantsDeleted::Yes, TrackReference::No);
    if (v) {
        if (v->getCas() != it.getCas()) {
            return;
        }

        if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            bool deleted = deleteStoredValue(hbl, *v);
            if (!deleted) {
                throw std::logic_error(
                        "VBucket::deleteExpiredItem: "
                        "Failed to delete seqno:" +
                        std::to_string(v->getBySeqno()) + " from bucket " +
                        std::to_string(hbl.getBucketNum()));
            }
        } else if (v->isExpired(startTime) && !v->isDeleted()) {
            VBNotifyCtx notifyCtx;
            ht.setValue(it, *v);
            std::tie(std::ignore, std::ignore, notifyCtx) =
                    processExpiredItem(hbl, *v);
            // we unlock ht lock here because we want to avoid potential lock
            // inversions arising from notifyNewSeqno() call
            hbl.getHTLock().unlock();
            notifyNewSeqno(notifyCtx);
        }
    } else {
        if (eviction == FULL_EVICTION) {
            // Create a temp item and delete and push it
            // into the checkpoint queue, only if the bloomfilter
            // predicts that the item may exist on disk.
            if (maybeKeyExistsInFilter(key)) {
                AddStatus rv = addTempStoredValue(hbl, key);
                if (rv == AddStatus::NoMem) {
                    return;
                }
                v = ht.unlocked_find(key,
                                     hbl.getBucketNum(),
                                     WantsDeleted::Yes,
                                     TrackReference::No);
                v->setDeleted();
                v->setRevSeqno(it.getRevSeqno());
                ht.setValue(it, *v);
                VBNotifyCtx notifyCtx;
                std::tie(std::ignore, std::ignore, notifyCtx) =
                        processExpiredItem(hbl, *v);
                // we unlock ht lock here because we want to avoid potential
                // lock inversions arising from notifyNewSeqno() call
                hbl.getHTLock().unlock();
                notifyNewSeqno(notifyCtx);
            }
        }
    }
    incExpirationStat(source);
}

ENGINE_ERROR_CODE VBucket::add(Item& itm,
                               const void* cookie,
                               EventuallyPersistentEngine& engine,
                               const int bgFetchDelay) {
    auto hbl = ht.getLockedBucket(itm.getKey());
    StoredValue* v = ht.unlocked_find(itm.getKey(),
                                      hbl.getBucketNum(),
                                      WantsDeleted::Yes,
                                      TrackReference::No);

    bool maybeKeyExists = true;
    if ((v == nullptr || v->isTempInitialItem()) &&
        (eviction == FULL_EVICTION)) {
        // Check bloomfilter's prediction
        if (!maybeKeyExistsInFilter(itm.getKey())) {
            maybeKeyExists = false;
        }
    }

    PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
    VBQueueItemCtx queueItmCtx(GenerateBySeqno::Yes,
                               GenerateCas::Yes,
                               TrackCasDrift::No,
                               /*isBackfillItem*/ false,
                               &preLinkDocumentContext);
    AddStatus status;
    VBNotifyCtx notifyCtx;
    std::tie(status, notifyCtx) =
            processAdd(hbl, v, itm, maybeKeyExists, false, queueItmCtx);

    switch (status) {
    case AddStatus::NoMem:
        return ENGINE_ENOMEM;
    case AddStatus::Exists:
        return ENGINE_NOT_STORED;
    case AddStatus::AddTmpAndBgFetch:
        return addTempItemAndBGFetch(
                hbl, itm.getKey(), cookie, engine, bgFetchDelay, true);
    case AddStatus::BgFetch:
        hbl.getHTLock().unlock();
        bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
        return ENGINE_EWOULDBLOCK;
    case AddStatus::Success:
    case AddStatus::UnDel:
        notifyNewSeqno(notifyCtx);
        itm.setBySeqno(v->getBySeqno());
        itm.setCas(v->getCas());
        break;
    }
    return ENGINE_SUCCESS;
}

std::pair<MutationStatus, GetValue> VBucket::processGetAndUpdateTtl(
        HashTable::HashBucketLock& hbl,
        const DocKey& key,
        StoredValue* v,
        time_t exptime) {
    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return {MutationStatus::NotFound, GetValue()};
        }

        if (!v->isResident()) {
            return {MutationStatus::NeedBgFetch, GetValue()};
        }

        if (v->isLocked(ep_current_time())) {
            return {MutationStatus::IsLocked,
                    GetValue(nullptr, ENGINE_KEY_EEXISTS, 0)};
        }

        const bool exptime_mutated = exptime != v->getExptime();
        auto bySeqNo = v->getBySeqno();
        if (exptime_mutated) {
            v->markDirty();
            v->setExptime(exptime);
            v->setRevSeqno(v->getRevSeqno() + 1);
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), getId()),
                    ENGINE_SUCCESS,
                    bySeqNo);

        if (exptime_mutated) {
            VBQueueItemCtx qItemCtx(GenerateBySeqno::Yes,
                                    GenerateCas::Yes,
                                    TrackCasDrift::No,
                                    false,
                                    nullptr);
            VBNotifyCtx notifyCtx;
            std::tie(v, std::ignore, notifyCtx) =
                    updateStoredValue(hbl, *v, *rv.item, qItemCtx, true);
            rv.item->setCas(v->getCas());
            // we unlock ht lock here because we want to avoid potential lock
            // inversions arising from notifyNewSeqno() call
            hbl.getHTLock().unlock();
            notifyNewSeqno(notifyCtx);
        }

        return {MutationStatus::WasClean, std::move(rv)};
    } else {
        if (eviction == VALUE_ONLY) {
            return {MutationStatus::NotFound, GetValue()};
        } else {
            if (maybeKeyExistsInFilter(key)) {
                return {MutationStatus::NeedBgFetch, GetValue()};
            } else {
                // As bloomfilter predicted that item surely doesn't exist
                // on disk, return ENOENT for getAndUpdateTtl().
                return {MutationStatus::NotFound, GetValue()};
            }
        }
    }
}

GetValue VBucket::getAndUpdateTtl(const DocKey& key,
                                  const void* cookie,
                                  EventuallyPersistentEngine& engine,
                                  int bgFetchDelay,
                                  time_t exptime) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl,
                                     key,
                                     WantsDeleted::Yes,
                                     TrackReference::Yes,
                                     QueueExpired::Yes);
    GetValue gv;
    MutationStatus status;
    std::tie(status, gv) = processGetAndUpdateTtl(hbl, key, v, exptime);

    if (status == MutationStatus::NeedBgFetch) {
        if (v) {
            bgFetch(key, cookie, engine, bgFetchDelay);
            return GetValue(nullptr, ENGINE_EWOULDBLOCK, v->getBySeqno());
        } else {
            ENGINE_ERROR_CODE ec = addTempItemAndBGFetch(
                    hbl, key, cookie, engine, bgFetchDelay, false);
            return GetValue(NULL, ec, -1, true);
        }
    }

    return gv;
}

GetValue VBucket::getInternal(const DocKey& key,
                              const void* cookie,
                              EventuallyPersistentEngine& engine,
                              int bgFetchDelay,
                              get_options_t options,
                              bool diskFlushAll,
                              GetKeyOnly getKeyOnly) {
    const TrackReference trackReference = (options & TRACK_REFERENCE)
                                                  ? TrackReference::Yes
                                                  : TrackReference::No;
    const bool metadataOnly = (options & ALLOW_META_ONLY);
    const bool getDeletedValue = (options & GET_DELETED_VALUE);
    const bool bgFetchRequired = (options & QUEUE_BG_FETCH);
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(
            hbl, key, WantsDeleted::Yes, trackReference, QueueExpired::Yes);
    if (v) {
        if (v->isDeleted() && !getDeletedValue) {
            return GetValue();
        }
        if (v->isTempDeletedItem() || v->isTempNonExistentItem()) {
            // Delete a temp non-existent item to ensure that
            // if the get were issued over an item that doesn't
            // exist, then we dont preserve a temp item.
            if (options & DELETE_TEMP) {
                deleteStoredValue(hbl, *v);
            }
            return GetValue();
        }

        // If the value is not resident (and it was requested), wait for it...
        if (!v->isResident() && !metadataOnly) {
            return getInternalNonResident(
                    key, cookie, engine, bgFetchDelay, options, *v);
        }

        // Should we hide (return -1) for the items' CAS?
        const bool hideCas =
                (options & HIDE_LOCKED_CAS) && v->isLocked(ep_current_time());
        std::unique_ptr<Item> item;
        if (getKeyOnly == GetKeyOnly::Yes) {
            item = v->toItemKeyOnly(getId());
        } else {
            item = v->toItem(hideCas, getId());
        }
        return GetValue(std::move(item),
                        ENGINE_SUCCESS,
                        v->getBySeqno(),
                        !v->isResident(),
                        v->getNRUValue());
    } else {
        if (!getDeletedValue && (eviction == VALUE_ONLY || diskFlushAll)) {
            return GetValue();
        }

        if (maybeKeyExistsInFilter(key)) {
            ENGINE_ERROR_CODE ec = ENGINE_EWOULDBLOCK;
            if (bgFetchRequired) { // Full eviction and need a bg fetch.
                ec = addTempItemAndBGFetch(
                        hbl, key, cookie, engine, bgFetchDelay, metadataOnly);
            }
            return GetValue(NULL, ec, -1, true);
        } else {
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT, for getInternal().
            return GetValue();
        }
    }
}

ENGINE_ERROR_CODE VBucket::getMetaData(const DocKey& key,
                                       const void* cookie,
                                       EventuallyPersistentEngine& engine,
                                       int bgFetchDelay,
                                       ItemMetaData& metadata,
                                       uint32_t& deleted,
                                       uint8_t& datatype) {
    deleted = 0;
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = ht.unlocked_find(
            key, hbl.getBucketNum(), WantsDeleted::Yes, TrackReference::No);

    if (v) {
        stats.numOpsGetMeta++;
        if (v->isTempInitialItem()) {
            // Need bg meta fetch.
            bgFetch(key, cookie, engine, bgFetchDelay, true);
            return ENGINE_EWOULDBLOCK;
        } else if (v->isTempNonExistentItem()) {
            metadata.cas = v->getCas();
            return ENGINE_KEY_ENOENT;
        } else {
            if (v->isTempDeletedItem() || v->isDeleted() ||
                v->isExpired(ep_real_time())) {
                deleted |= GET_META_ITEM_DELETED_FLAG;
            }

            if (v->isLocked(ep_current_time())) {
                metadata.cas = static_cast<uint64_t>(-1);
            } else {
                metadata.cas = v->getCas();
            }
            metadata.flags = v->getFlags();
            metadata.exptime = v->getExptime();
            metadata.revSeqno = v->getRevSeqno();
            datatype = v->getDatatype();

            return ENGINE_SUCCESS;
        }
    } else {
        // The key wasn't found. However, this may be because it was previously
        // deleted or evicted with the full eviction strategy.
        // So, add a temporary item corresponding to the key to the hash table
        // and schedule a background fetch for its metadata from the persistent
        // store. The item's state will be updated after the fetch completes.
        //
        // Schedule this bgFetch only if the key is predicted to be may-be
        // existent on disk by the bloomfilter.

        if (maybeKeyExistsInFilter(key)) {
            return addTempItemAndBGFetch(
                    hbl, key, cookie, engine, bgFetchDelay, true);
        } else {
            stats.numOpsGetMeta++;
            return ENGINE_KEY_ENOENT;
        }
    }
}

ENGINE_ERROR_CODE VBucket::getKeyStats(const DocKey& key,
                                       const void* cookie,
                                       EventuallyPersistentEngine& engine,
                                       int bgFetchDelay,
                                       struct key_stats& kstats,
                                       WantsDeleted wantsDeleted) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl,
                                     key,
                                     WantsDeleted::Yes,
                                     TrackReference::Yes,
                                     QueueExpired::Yes);

    if (v) {
        if ((v->isDeleted() && wantsDeleted == WantsDeleted::No) ||
            v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            return ENGINE_KEY_ENOENT;
        }
        if (eviction == FULL_EVICTION && v->isTempInitialItem()) {
            hbl.getHTLock().unlock();
            bgFetch(key, cookie, engine, bgFetchDelay, true);
            return ENGINE_EWOULDBLOCK;
        }
        kstats.logically_deleted = v->isDeleted();
        kstats.dirty = v->isDirty();
        kstats.exptime = v->getExptime();
        kstats.flags = v->getFlags();
        kstats.cas = v->getCas();
        kstats.vb_state = getState();
        return ENGINE_SUCCESS;
    } else {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else {
            if (maybeKeyExistsInFilter(key)) {
                return addTempItemAndBGFetch(
                        hbl, key, cookie, engine, bgFetchDelay, true);
            } else {
                // If bgFetch were false, or bloomfilter predicted that
                // item surely doesn't exist on disk, return ENOENT for
                // getKeyStats().
                return ENGINE_KEY_ENOENT;
            }
        }
    }
}

GetValue VBucket::getLocked(const DocKey& key,
                            rel_time_t currentTime,
                            uint32_t lockTimeout,
                            const void* cookie,
                            EventuallyPersistentEngine& engine,
                            int bgFetchDelay) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = fetchValidValue(hbl,
                                     key,
                                     WantsDeleted::Yes,
                                     TrackReference::Yes,
                                     QueueExpired::Yes);

    if (v) {
        if (v->isDeleted() || v->isTempNonExistentItem() ||
            v->isTempDeletedItem()) {
            return GetValue(NULL, ENGINE_KEY_ENOENT);
        }

        // if v is locked return error
        if (v->isLocked(currentTime)) {
            return GetValue(NULL, ENGINE_TMPFAIL);
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (cookie) {
                bgFetch(key, cookie, engine, bgFetchDelay);
            }
            return GetValue(NULL, ENGINE_EWOULDBLOCK, -1, true);
        }

        // acquire lock and increment cas value
        v->lock(currentTime + lockTimeout);

        auto it = v->toItem(false, getId());
        it->setCas(nextHLCCas());
        v->setCas(it->getCas());

        return GetValue(std::move(it));

    } else {
        // No value found in the hashtable.
        switch (eviction) {
        case VALUE_ONLY:
            return GetValue(NULL, ENGINE_KEY_ENOENT);

        case FULL_EVICTION:
            if (maybeKeyExistsInFilter(key)) {
                ENGINE_ERROR_CODE ec = addTempItemAndBGFetch(
                        hbl, key, cookie, engine, bgFetchDelay, false);
                return GetValue(NULL, ec, -1, true);
            } else {
                // As bloomfilter predicted that item surely doesn't exist
                // on disk, return ENOENT for getLocked().
                return GetValue(NULL, ENGINE_KEY_ENOENT);
            }
        }
        return GetValue(); // just to prevent compiler warning
    }
}

void VBucket::deletedOnDiskCbk(const Item& queuedItem, bool deleted) {
    auto hbl = ht.getLockedBucket(queuedItem.getKey());
    StoredValue* v = fetchValidValue(hbl,
                                     queuedItem.getKey(),
                                     WantsDeleted::Yes,
                                     TrackReference::No,
                                     QueueExpired::Yes);
    // Delete the item in the hash table iff:
    //  1. Item is existent in hashtable, and deleted flag is true
    //  2. rev seqno of queued item matches rev seqno of hash table item
    if (v && v->isDeleted() && (queuedItem.getRevSeqno() == v->getRevSeqno())) {
        bool isDeleted = deleteStoredValue(hbl, *v);
        if (!isDeleted) {
            throw std::logic_error(
                    "deletedOnDiskCbk:callback: "
                    "Failed to delete key with seqno:" +
                    std::to_string(v->getBySeqno()) + "' from bucket " +
                    std::to_string(hbl.getBucketNum()));
        }

        /**
         * Deleted items are to be added to the bloomfilter,
         * in either eviction policy.
         */
        addToFilter(queuedItem.getKey());
    }

    if (deleted) {
        ++stats.totalPersisted;
        ++opsDelete;
    }
    doStatsForFlushing(queuedItem, queuedItem.size());
    --stats.diskQueueSize;
    decrMetaDataDisk(queuedItem);
}

bool VBucket::deleteKey(const DocKey& key) {
    auto hbl = ht.getLockedBucket(key);
    StoredValue* v = ht.unlocked_find(
            key, hbl.getBucketNum(), WantsDeleted::Yes, TrackReference::No);
    if (!v) {
        return false;
    }
    return deleteStoredValue(hbl, *v);
}

void VBucket::postProcessRollback(const RollbackResult& rollbackResult,
                                  uint64_t prevHighSeqno) {
    failovers->pruneEntries(rollbackResult.highSeqno);
    checkpointManager.clear(*this, rollbackResult.highSeqno);
    setPersistedSnapshot(rollbackResult.snapStartSeqno,
                         rollbackResult.snapEndSeqno);
    incrRollbackItemCount(prevHighSeqno - rollbackResult.highSeqno);
    checkpointManager.setOpenCheckpointId(1);
}

void VBucket::dump() const {
    std::cerr << "VBucket[" << this << "] with state: " << toString(getState())
              << " numItems:" << getNumItems()
              << " numNonResident:" << getNumNonResidentItems()
              << " ht: " << std::endl << "  " << ht << std::endl
              << "]" << std::endl;
}

void VBucket::_addStats(bool details, ADD_STAT add_stat, const void* c) {
    addStat(NULL, toString(state), add_stat, c);
    if (details) {
        size_t numItems = getNumItems();
        size_t tempItems = getNumTempItems();
        addStat("num_items", numItems, add_stat, c);
        addStat("num_temp_items", tempItems, add_stat, c);
        addStat("num_non_resident", getNumNonResidentItems(), add_stat, c);
        addStat("ht_memory", ht.memorySize(), add_stat, c);
        addStat("ht_item_memory", ht.getItemMemory(), add_stat, c);
        addStat("ht_cache_size", ht.cacheSize.load(), add_stat, c);
        addStat("ht_size", ht.getSize(), add_stat, c);
        addStat("num_ejects", ht.getNumEjects(), add_stat, c);
        addStat("ops_create", opsCreate.load(), add_stat, c);
        addStat("ops_update", opsUpdate.load(), add_stat, c);
        addStat("ops_delete", opsDelete.load(), add_stat, c);
        addStat("ops_reject", opsReject.load(), add_stat, c);
        addStat("queue_size", dirtyQueueSize.load(), add_stat, c);
        addStat("backfill_queue_size", getBackfillSize(), add_stat, c);
        addStat("queue_memory", dirtyQueueMem.load(), add_stat, c);
        addStat("queue_fill", dirtyQueueFill.load(), add_stat, c);
        addStat("queue_drain", dirtyQueueDrain.load(), add_stat, c);
        addStat("queue_age", getQueueAge(), add_stat, c);
        addStat("pending_writes", dirtyQueuePendingWrites.load(), add_stat, c);

        addStat("high_seqno", getHighSeqno(), add_stat, c);
        addStat("uuid", failovers->getLatestUUID(), add_stat, c);
        addStat("purge_seqno", getPurgeSeqno(), add_stat, c);
        addStat("bloom_filter", getFilterStatusString().data(),
                add_stat, c);
        addStat("bloom_filter_size", getFilterSize(), add_stat, c);
        addStat("bloom_filter_key_count", getNumOfKeysInFilter(), add_stat, c);
        addStat("rollback_item_count", getRollbackItemCount(), add_stat, c);
        addStat("hp_vb_req_size", getHighPriorityChkSize(), add_stat, c);
        hlc.addStats(statPrefix, add_stat, c);
    }
}

void VBucket::decrDirtyQueueMem(size_t decrementBy)
{
    size_t oldVal, newVal;
    do {
        oldVal = dirtyQueueMem.load(std::memory_order_relaxed);
        if (oldVal < decrementBy) {
            newVal = 0;
        } else {
            newVal = oldVal - decrementBy;
        }
    } while (!dirtyQueueMem.compare_exchange_strong(oldVal, newVal));
}

void VBucket::decrDirtyQueueAge(uint32_t decrementBy)
{
    uint64_t oldVal, newVal;
    do {
        oldVal = dirtyQueueAge.load(std::memory_order_relaxed);
        if (oldVal < decrementBy) {
            newVal = 0;
        } else {
            newVal = oldVal - decrementBy;
        }
    } while (!dirtyQueueAge.compare_exchange_strong(oldVal, newVal));
}

void VBucket::decrDirtyQueuePendingWrites(size_t decrementBy)
{
    size_t oldVal, newVal;
    do {
        oldVal = dirtyQueuePendingWrites.load(std::memory_order_relaxed);
        if (oldVal < decrementBy) {
            newVal = 0;
        } else {
            newVal = oldVal - decrementBy;
        }
    } while (!dirtyQueuePendingWrites.compare_exchange_strong(oldVal, newVal));
}

std::pair<MutationStatus, VBNotifyCtx> VBucket::processSet(
        const HashTable::HashBucketLock& hbl,
        StoredValue*& v,
        Item& itm,
        uint64_t cas,
        bool allowExisting,
        bool hasMetaData,
        const VBQueueItemCtx& queueItmCtx,
        bool maybeKeyExists,
        bool isReplication) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::processSet: htLock not held for "
                "VBucket " +
                std::to_string(getId()));
    }

    if (!StoredValue::hasAvailableSpace(stats, itm, isReplication)) {
        return {MutationStatus::NoMem, VBNotifyCtx()};
    }

    if (cas && eviction == FULL_EVICTION && maybeKeyExists) {
        if (!v || v->isTempInitialItem()) {
            return {MutationStatus::NeedBgFetch, VBNotifyCtx()};
        }
    }

    /*
     * prior to checking for the lock, we should check if this object
     * has expired. If so, then check if CAS value has been provided
     * for this set op. In this case the operation should be denied since
     * a cas operation for a key that doesn't exist is not a very cool
     * thing to do. See MB 3252
     */
    if (v && v->isExpired(ep_real_time()) && !hasMetaData && !itm.isDeleted()) {
        if (v->isLocked(ep_current_time())) {
            v->unlock();
        }
        if (cas) {
            /* item has expired and cas value provided. Deny ! */
            return {MutationStatus::NotFound, VBNotifyCtx()};
        }
    }

    if (v) {
        if (!allowExisting && !v->isTempItem() && !v->isDeleted()) {
            return {MutationStatus::InvalidCas, VBNotifyCtx()};
        }
        if (v->isLocked(ep_current_time())) {
            /*
             * item is locked, deny if there is cas value mismatch
             * or no cas value is provided by the user
             */
            if (cas != v->getCas()) {
                return {MutationStatus::IsLocked, VBNotifyCtx()};
            }
            /* allow operation*/
            v->unlock();
        } else if (cas && cas != v->getCas()) {
            if (v->isTempNonExistentItem()) {
                // This is a temporary item which marks a key as non-existent;
                // therefore specifying a non-matching CAS should be exposed
                // as item not existing.
                return {MutationStatus::NotFound, VBNotifyCtx()};
            }
            if ((v->isTempDeletedItem() || v->isDeleted()) && !itm.isDeleted()) {
                // Existing item is deleted, and we are not replacing it with
                // a (different) deleted value - return not existing.
                return {MutationStatus::NotFound, VBNotifyCtx()};
            }
            // None of the above special cases; the existing item cannot be
            // modified with the specified CAS.
            return {MutationStatus::InvalidCas, VBNotifyCtx()};
        }
        if (!hasMetaData) {
            itm.setRevSeqno(v->getRevSeqno() + 1);
            /* MB-23530: We must ensure that a replace operation (i.e.
             * set with a CAS) /fails/ if the old document is deleted; it
             * logically "doesn't exist". However, if the new value is deleted
             * this op is a /delete/ with a CAS and we must permit a
             * deleted -> deleted transition for Deleted Bodies.
             */
            if (cas && (v->isDeleted() || v->isTempDeletedItem()) &&
                !itm.isDeleted()) {
                return {MutationStatus::NotFound, VBNotifyCtx()};
            }
        }

        MutationStatus status;
        VBNotifyCtx notifyCtx;
        std::tie(v, status, notifyCtx) =
                updateStoredValue(hbl, *v, itm, queueItmCtx);
        return {status, notifyCtx};
    } else if (cas != 0) {
        return {MutationStatus::NotFound, VBNotifyCtx()};
    } else {
        VBNotifyCtx notifyCtx;
        std::tie(v, notifyCtx) = addNewStoredValue(hbl, itm, queueItmCtx);
        if (!hasMetaData) {
            updateRevSeqNoOfNewStoredValue(*v);
            itm.setRevSeqno(v->getRevSeqno());
        }
        return {MutationStatus::WasClean, notifyCtx};
    }
}

std::pair<AddStatus, VBNotifyCtx> VBucket::processAdd(
        const HashTable::HashBucketLock& hbl,
        StoredValue*& v,
        Item& itm,
        bool maybeKeyExists,
        bool isReplication,
        const VBQueueItemCtx& queueItmCtx) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::processAdd: htLock not held for "
                "VBucket " +
                std::to_string(getId()));
    }

    if (v && !v->isDeleted() && !v->isExpired(ep_real_time()) &&
        !v->isTempItem()) {
        return {AddStatus::Exists, VBNotifyCtx()};
    }
    if (!StoredValue::hasAvailableSpace(stats, itm, isReplication)) {
        return {AddStatus::NoMem, VBNotifyCtx()};
    }

    std::pair<AddStatus, VBNotifyCtx> rv = {AddStatus::Success, VBNotifyCtx()};

    if (v) {
        if (v->isTempInitialItem() && eviction == FULL_EVICTION &&
            maybeKeyExists) {
            // Need to figure out if an item exists on disk
            return {AddStatus::BgFetch, VBNotifyCtx()};
        }

        rv.first = (v->isDeleted() || v->isExpired(ep_real_time()))
                           ? AddStatus::UnDel
                           : AddStatus::Success;

        if (v->isTempDeletedItem()) {
            itm.setRevSeqno(v->getRevSeqno() + 1);
        } else {
            itm.setRevSeqno(ht.getMaxDeletedRevSeqno() + 1);
        }

        if (!v->isTempItem()) {
            itm.setRevSeqno(v->getRevSeqno() + 1);
        }

        std::tie(v, std::ignore, rv.second) =
                updateStoredValue(hbl, *v, itm, queueItmCtx);
    } else {
        if (itm.getBySeqno() != StoredValue::state_temp_init) {
            if (eviction == FULL_EVICTION && maybeKeyExists) {
                return {AddStatus::AddTmpAndBgFetch, VBNotifyCtx()};
            }
        }

        if (itm.getBySeqno() == StoredValue::state_temp_init) {
            /* A 'temp initial item' is just added to the hash table. It is
             not put on checkpoint manager or sequence list */
            v = ht.unlocked_addNewStoredValue(hbl, itm);
        } else {
            std::tie(v, rv.second) = addNewStoredValue(hbl, itm, queueItmCtx);
        }

        updateRevSeqNoOfNewStoredValue(*v);
        itm.setRevSeqno(v->getRevSeqno());
        if (v->isTempItem()) {
            rv.first = AddStatus::BgFetch;
        }
    }

    if (v->isTempItem()) {
        v->setNRUValue(MAX_NRU_VALUE);
    }
    return rv;
}

std::tuple<MutationStatus, StoredValue*, VBNotifyCtx>
VBucket::processSoftDelete(const HashTable::HashBucketLock& hbl,
                           StoredValue& v,
                           uint64_t cas,
                           const ItemMetaData& metadata,
                           const VBQueueItemCtx& queueItmCtx,
                           bool use_meta,
                           uint64_t bySeqno) {
    if (v.isTempInitialItem() && eviction == FULL_EVICTION) {
        return std::make_tuple(MutationStatus::NeedBgFetch, &v, VBNotifyCtx());
    }

    if (v.isLocked(ep_current_time())) {
        if (cas != v.getCas()) {
            return std::make_tuple(MutationStatus::IsLocked, &v, VBNotifyCtx());
        }
        v.unlock();
    }

    if (cas != 0 && cas != v.getCas()) {
        return std::make_tuple(MutationStatus::InvalidCas, &v, VBNotifyCtx());
    }

    /* allow operation */
    v.unlock();

    MutationStatus rv =
            v.isDirty() ? MutationStatus::WasDirty : MutationStatus::WasClean;

    if (use_meta) {
        v.setCas(metadata.cas);
        v.setFlags(metadata.flags);
        v.setExptime(metadata.exptime);
    }

    v.setRevSeqno(metadata.revSeqno);
    VBNotifyCtx notifyCtx;
    StoredValue* newSv;
    std::tie(newSv, notifyCtx) =
            softDeleteStoredValue(hbl,
                                  v,
                                  /*onlyMarkDeleted*/ false,
                                  queueItmCtx,
                                  bySeqno);
    ht.updateMaxDeletedRevSeqno(metadata.revSeqno);
    return std::make_tuple(rv, newSv, notifyCtx);
}

std::tuple<MutationStatus, StoredValue*, VBNotifyCtx>
VBucket::processExpiredItem(const HashTable::HashBucketLock& hbl,
                            StoredValue& v) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::processExpiredItem: htLock not held for VBucket " +
                std::to_string(getId()));
    }

    if (v.isTempInitialItem() && eviction == FULL_EVICTION) {
        return std::make_tuple(MutationStatus::NeedBgFetch,
                               &v,
                               queueDirty(v,
                                          GenerateBySeqno::Yes,
                                          GenerateCas::Yes,
                                          /*isBackfillItem*/ false));
    }

    /* If the datatype is XATTR, mark the item as deleted
     * but don't delete the value as system xattrs can
     * still be queried by mobile clients even after
     * deletion.
     * TODO: The current implementation is inefficient
     * but functionally correct and for performance reasons
     * only the system xattrs need to be stored.
     */
    value_t value = v.getValue();
    bool onlyMarkDeleted =
            value && mcbp::datatype::is_xattr(value->getDataType());
    v.setRevSeqno(v.getRevSeqno() + 1);
    VBNotifyCtx notifyCtx;
    StoredValue* newSv;
    std::tie(newSv, notifyCtx) =
            softDeleteStoredValue(hbl,
                                  v,
                                  onlyMarkDeleted,
                                  VBQueueItemCtx(GenerateBySeqno::Yes,
                                                 GenerateCas::Yes,
                                                 TrackCasDrift::No,
                                                 /*isBackfillItem*/ false,
                                                 nullptr /* no pre link */),
                                  v.getBySeqno());
    ht.updateMaxDeletedRevSeqno(newSv->getRevSeqno() + 1);
    return std::make_tuple(MutationStatus::NotFound, newSv, notifyCtx);
}

bool VBucket::deleteStoredValue(const HashTable::HashBucketLock& hbl,
                                StoredValue& v) {
    if (!v.isDeleted() && v.isLocked(ep_current_time())) {
        return false;
    }

    /* StoredValue deleted here. If any other in-memory data structures are
       using the StoredValue intrusively then they must have handled the delete
       by this point */
    ht.unlocked_del(hbl, v.getKey());
    return true;
}

AddStatus VBucket::addTempStoredValue(const HashTable::HashBucketLock& hbl,
                                      const DocKey& key,
                                      bool isReplication) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::addTempStoredValue: htLock not held for "
                "VBucket " +
                std::to_string(getId()));
    }

    uint8_t ext_meta[EXT_META_LEN] = {PROTOCOL_BINARY_RAW_BYTES};
    static_assert(sizeof(ext_meta) == 1,
                  "VBucket::addTempStoredValue(): expected "
                  "EXT_META_LEN to be 1");
    Item itm(key,
             /*flags*/ 0,
             /*exp*/ 0,
             /*data*/ NULL,
             /*size*/ 0,
             ext_meta,
             sizeof(ext_meta),
             0,
             StoredValue::state_temp_init);

    if (!StoredValue::hasAvailableSpace(stats, itm, isReplication)) {
        return AddStatus::NoMem;
    }

    /* A 'temp initial item' is just added to the hash table. It is
       not put on checkpoint manager or sequence list */
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

    updateRevSeqNoOfNewStoredValue(*v);
    itm.setRevSeqno(v->getRevSeqno());
    v->setNRUValue(MAX_NRU_VALUE);

    return AddStatus::BgFetch;
}

void VBucket::notifyNewSeqno(const VBNotifyCtx& notifyCtx) {
    if (newSeqnoCb) {
        newSeqnoCb->callback(getId(), notifyCtx);
    }
}

/*
 * Queue the item to the checkpoint and return the seqno the item was
 * allocated.
 */
int64_t VBucket::queueItem(Item* item, OptionalSeqno seqno) {
    item->setVBucketId(id);
    queued_item qi(item);
    checkpointManager.queueDirty(
            *this,
            qi,
            seqno ? GenerateBySeqno::No : GenerateBySeqno::Yes,
            GenerateCas::Yes,
            nullptr /* No pre link step as this is for system events */);
    VBNotifyCtx notifyCtx;
    // If the seqno is initialized, skip replication notification
    notifyCtx.notifyReplication = !seqno.is_initialized();
    notifyCtx.notifyFlusher = true;
    notifyCtx.bySeqno = qi->getBySeqno();
    notifyNewSeqno(notifyCtx);
    return qi->getBySeqno();
}

VBNotifyCtx VBucket::queueDirty(StoredValue& v,
                                const VBQueueItemCtx& queueItmCtx) {
    if (queueItmCtx.trackCasDrift == TrackCasDrift::Yes) {
        setMaxCasAndTrackDrift(v.getCas());
    }
    return queueDirty(v,
                      queueItmCtx.genBySeqno,
                      queueItmCtx.genCas,
                      queueItmCtx.isBackfillItem,
                      queueItmCtx.preLinkDocumentContext);
}

void VBucket::updateRevSeqNoOfNewStoredValue(StoredValue& v) {
    /**
     * Possibly, this item is being recreated. Conservatively assign it
     * a seqno that is greater than the greatest seqno of all deleted
     * items seen so far.
     */
    uint64_t seqno = ht.getMaxDeletedRevSeqno();
    if (!v.isTempItem()) {
        ++seqno;
    }
    v.setRevSeqno(seqno);
}

void VBucket::addHighPriorityVBEntry(uint64_t seqnoOrChkId,
                                     const void* cookie,
                                     HighPriorityVBNotify reqType) {
    std::unique_lock<std::mutex> lh(hpVBReqsMutex);
    hpVBReqs.push_back(HighPriorityVBEntry(cookie, seqnoOrChkId, reqType));
    numHpVBReqs.store(hpVBReqs.size());

    LOG(EXTENSION_LOG_NOTICE,
        "Added high priority async request %s "
        "for vb:%" PRIu16 ", Check for:%" PRIu64 ", "
        "Persisted upto:%" PRIu64 ", cookie:%p",
        to_string(reqType).c_str(),
        getId(),
        seqnoOrChkId,
        getPersistenceSeqno(),
        cookie);
}

std::map<const void*, ENGINE_ERROR_CODE> VBucket::getHighPriorityNotifications(
        EventuallyPersistentEngine& engine,
        uint64_t idNum,
        HighPriorityVBNotify notifyType) {
    std::unique_lock<std::mutex> lh(hpVBReqsMutex);
    std::map<const void*, ENGINE_ERROR_CODE> toNotify;

    auto entry = hpVBReqs.begin();

    while (entry != hpVBReqs.end()) {
        if (notifyType != entry->reqType) {
            ++entry;
            continue;
        }

        std::string logStr(to_string(notifyType));

        hrtime_t wall_time(gethrtime() - entry->start);
        size_t spent = wall_time / 1000000000;
        if (entry->id <= idNum) {
            toNotify[entry->cookie] = ENGINE_SUCCESS;
            stats.chkPersistenceHisto.add(wall_time / 1000);
            adjustCheckpointFlushTimeout(wall_time / 1000000000);
            LOG(EXTENSION_LOG_NOTICE,
                "Notified the completion of %s "
                "for vbucket %" PRIu16 ", Check for: %" PRIu64
                ", "
                "Persisted upto: %" PRIu64 ", cookie %p",
                logStr.c_str(),
                getId(),
                entry->id,
                idNum,
                entry->cookie);
            entry = hpVBReqs.erase(entry);
        } else if (spent > getCheckpointFlushTimeout()) {
            adjustCheckpointFlushTimeout(spent);
            engine.storeEngineSpecific(entry->cookie, NULL);
            toNotify[entry->cookie] = ENGINE_TMPFAIL;
            LOG(EXTENSION_LOG_WARNING,
                "Notified the timeout on %s "
                "for vbucket %" PRIu16 ", Check for: %" PRIu64
                ", "
                "Persisted upto: %" PRIu64 ", cookie %p",
                logStr.c_str(),
                getId(),
                entry->id,
                idNum,
                entry->cookie);
            entry = hpVBReqs.erase(entry);
        } else {
            ++entry;
        }
    }
    numHpVBReqs.store(hpVBReqs.size());
    return toNotify;
}

std::map<const void*, ENGINE_ERROR_CODE> VBucket::tmpFailAndGetAllHpNotifies(
        EventuallyPersistentEngine& engine) {
    std::map<const void*, ENGINE_ERROR_CODE> toNotify;

    LockHolder lh(hpVBReqsMutex);

    for (auto& entry : hpVBReqs) {
        toNotify[entry.cookie] = ENGINE_TMPFAIL;
        engine.storeEngineSpecific(entry.cookie, NULL);
    }
    hpVBReqs.clear();

    return toNotify;
}

void VBucket::adjustCheckpointFlushTimeout(size_t wall_time) {
    size_t middle = (MIN_CHK_FLUSH_TIMEOUT + MAX_CHK_FLUSH_TIMEOUT) / 2;

    if (wall_time <= MIN_CHK_FLUSH_TIMEOUT) {
        chkFlushTimeout = MIN_CHK_FLUSH_TIMEOUT;
    } else if (wall_time <= middle) {
        chkFlushTimeout = middle;
    } else {
        chkFlushTimeout = MAX_CHK_FLUSH_TIMEOUT;
    }
}

size_t VBucket::getCheckpointFlushTimeout() {
    return chkFlushTimeout;
}

std::unique_ptr<Item> VBucket::pruneXattrDocument(
        StoredValue& v, const ItemMetaData& itemMeta) {
    // Need to take a copy of the value, prune it, and add it back

    // Create work-space document
    std::vector<uint8_t> workspace(v.getValue()->vlength());
    std::copy_n(v.getValue()->getData(),
                v.getValue()->vlength(),
                workspace.begin());

    // Now attach to the XATTRs in the document
    auto sz = cb::xattr::get_body_offset(
            {reinterpret_cast<char*>(workspace.data()), workspace.size()});

    cb::xattr::Blob xattr({workspace.data(), sz});
    xattr.prune_user_keys();

    auto prunedXattrs = xattr.finalize();

    if (prunedXattrs.size()) {
        // Something remains - Create a Blob and copy-in just the XATTRs
        auto newValue =
                Blob::New(reinterpret_cast<const char*>(prunedXattrs.data()),
                          prunedXattrs.size(),
                          const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(
                                  v.getValue()->getExtMeta())),
                          v.getValue()->getExtLen());

        return std::make_unique<Item>(v.getKey(),
                                      itemMeta.flags,
                                      itemMeta.exptime,
                                      newValue,
                                      itemMeta.cas,
                                      v.getBySeqno(),
                                      getId(),
                                      itemMeta.revSeqno);
    } else {
        return {};
    }
}

void VBucket::DeferredDeleter::operator()(VBucket* vb) const {
    // If the vbucket is marked as deleting then we must schedule task to
    // perform the resource destruction (memory/disk).
    if (vb->isDeletionDeferred()) {
        vb->scheduleDeferredDeletion(engine);
        return;
    }
    delete vb;
}
