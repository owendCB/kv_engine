/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "durability_monitor_test.h"

#include "checkpoint_manager.h"
#include "test_helpers.h"

#include "../mock/mock_synchronous_ep_engine.h"

void DurabilityMonitorTest::addSyncWrite(int64_t seqno,
                                         cb::durability::Requirements req) {
    auto numTracked = monitor->public_getNumTracked();
    auto item = Item(makeStoredDocKey("key" + std::to_string(seqno)),
                     0 /*flags*/,
                     0 /*exp*/,
                     "value",
                     5 /*valueSize*/,
                     PROTOCOL_BINARY_RAW_BYTES,
                     0 /*cas*/,
                     seqno);
    using namespace cb::durability;
    item.setPendingSyncWrite(req);
    // Note: necessary for non-auto-generated seqno
    vb->checkpointManager->createSnapshot(seqno, seqno);
    // Note: need to go through VBucket::processSet to set the given bySeqno
    ASSERT_EQ(MutationStatus::WasClean, processSet(item));
    ASSERT_EQ(numTracked + 1, monitor->public_getNumTracked());
}

size_t DurabilityMonitorTest::addSyncWrites(int64_t seqnoStart,
                                            int64_t seqnoEnd,
                                            cb::durability::Requirements req) {
    size_t expectedNumTracked = monitor->public_getNumTracked();
    size_t added = 0;
    for (auto seqno = seqnoStart; seqno <= seqnoEnd; seqno++) {
        addSyncWrite(seqno, req);
        added++;
        expectedNumTracked++;
        EXPECT_EQ(expectedNumTracked, monitor->public_getNumTracked());
    }
    return added;
}

size_t DurabilityMonitorTest::addSyncWrites(const std::vector<int64_t>& seqnos,
                                            cb::durability::Requirements req) {
    if (seqnos.empty()) {
        throw std::logic_error(
                "DurabilityMonitorTest::addSyncWrites: seqnos list is empty");
    }
    size_t expectedNumTracked = monitor->public_getNumTracked();
    size_t added = 0;
    for (auto seqno : seqnos) {
        addSyncWrite(seqno, req);
        added++;
        expectedNumTracked++;
        EXPECT_EQ(expectedNumTracked, monitor->public_getNumTracked());
    }
    return added;
}

MutationStatus DurabilityMonitorTest::processSet(Item& item) {
    auto htRes = vb->ht.findForWrite(item.getKey());
    VBQueueItemCtx ctx;
    ctx.genBySeqno = GenerateBySeqno::No;
    ctx.durability =
            DurabilityItemCtx{item.getDurabilityReqs(), /*cookie*/ nullptr};
    return vb
            ->processSet(htRes.lock,
                         htRes.storedValue,
                         item,
                         item.getCas(),
                         true /*allow_existing*/,
                         false /*has_metadata*/,
                         ctx,
                         {/*no predicate*/})
            .first;
}

void DurabilityMonitorTest::assertNodeMemTracking(const std::string& node,
                                                  uint64_t lastWriteSeqno,
                                                  uint64_t lastAckSeqno) {
    ASSERT_EQ(lastWriteSeqno, monitor->public_getNodeWriteSeqnos(node).memory);
    ASSERT_EQ(lastAckSeqno, monitor->public_getNodeAckSeqnos(node).memory);
}

void DurabilityMonitorTest::assertNodeDiskTracking(const std::string& node,
                                                   uint64_t lastWriteSeqno,
                                                   uint64_t lastAckSeqno) {
    ASSERT_EQ(lastWriteSeqno, monitor->public_getNodeWriteSeqnos(node).disk);
    ASSERT_EQ(lastAckSeqno, monitor->public_getNodeAckSeqnos(node).disk);
}

TEST_F(DurabilityMonitorTest, AddSyncWrite) {
    EXPECT_EQ(3, addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/));
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedSmallerThanLastAcked) {
    addSyncWrites({1, 2} /*seqnos*/);

    // This call removes seqno:1
    ASSERT_NO_THROW(monitor->seqnoAckReceived(
            replica, 1 /*memSeqno*/, 0 /*diskSeqno*/));
    ASSERT_EQ(1, monitor->public_getNumTracked());
    ASSERT_EQ(1, monitor->public_getNodeWriteSeqnos(replica).memory);
    ASSERT_EQ(1, monitor->public_getNodeAckSeqnos(replica).memory);

    try {
        monitor->seqnoAckReceived(replica, 0 /*memSeqno*/, 0 /*diskSeqno*/);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("Monotonic") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedEqualPending) {
    int64_t seqnoStart = 1;
    int64_t seqnoEnd = 3;
    auto numItems = addSyncWrites(seqnoStart, seqnoEnd);
    ASSERT_EQ(3, numItems);
    ASSERT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).memory);
    ASSERT_EQ(0, monitor->public_getNodeAckSeqnos(replica).memory);

    for (int64_t seqno = seqnoStart; seqno <= seqnoEnd; seqno++) {
        EXPECT_NO_THROW(monitor->seqnoAckReceived(
                replica, seqno /*memSeqno*/, 0 /*diskSeqno*/));
        // Check that the tracking advances by 1 at each cycle
        EXPECT_EQ(seqno, monitor->public_getNodeWriteSeqnos(replica).memory);
        EXPECT_EQ(seqno, monitor->public_getNodeAckSeqnos(replica).memory);
        // Check that we committed and removed 1 SyncWrite
        EXPECT_EQ(--numItems, monitor->public_getNumTracked());
        // Check that seqno-tracking is not lost after commit+remove
        EXPECT_EQ(seqno, monitor->public_getNodeWriteSeqnos(replica).memory);
        EXPECT_EQ(seqno, monitor->public_getNodeAckSeqnos(replica).memory);
    }
}

TEST_F(DurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanPending_ContinuousSeqnos) {
    ASSERT_EQ(3, addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/));
    ASSERT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).memory);

    int64_t memoryAckSeqno = 2;
    // Receive a seqno-ack in the middle of tracked seqnos
    EXPECT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(
                      replica, memoryAckSeqno, 0 /*diskSeqno*/));
    // Check that the tracking has advanced to the ack'ed seqno
    EXPECT_EQ(memoryAckSeqno,
              monitor->public_getNodeWriteSeqnos(replica).memory);
    EXPECT_EQ(memoryAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
    // Check that we committed and removed 2 SyncWrites
    EXPECT_EQ(1, monitor->public_getNumTracked());
    // Check that seqno-tracking is not lost after commit+remove
    EXPECT_EQ(memoryAckSeqno,
              monitor->public_getNodeWriteSeqnos(replica).memory);
    EXPECT_EQ(memoryAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedGreaterThanPending_SparseSeqnos) {
    ASSERT_EQ(3, addSyncWrites({1, 3, 5} /*seqnos*/));
    ASSERT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).memory);

    int64_t memoryAckSeqno = 4;
    // Receive a seqno-ack in the middle of tracked seqnos
    EXPECT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(
                      replica, memoryAckSeqno, 0 /*diskSeqno*/));
    // Check that the tracking has advanced to the last tracked seqno before
    // the ack'ed seqno
    EXPECT_EQ(3, monitor->public_getNodeWriteSeqnos(replica).memory);
    // Check that the ack-seqno has been updated correctly
    EXPECT_EQ(memoryAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
    // Check that we committed and removed 2 SyncWrites
    EXPECT_EQ(1, monitor->public_getNumTracked());
    // Check that seqno-tracking is not lost after commit+remove
    EXPECT_EQ(3, monitor->public_getNodeWriteSeqnos(replica).memory);
    EXPECT_EQ(memoryAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
}

TEST_F(DurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanLastTracked_ContinuousSeqnos) {
    ASSERT_EQ(3, addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/));
    ASSERT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).memory);

    int64_t memoryAckSeqno = 4;
    // Receive a seqno-ack greater than the last tracked seqno
    EXPECT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(
                      replica, memoryAckSeqno, 0 /*diskSeqno*/));
    // Check that the tracking has advanced to the last tracked seqno
    EXPECT_EQ(3, monitor->public_getNodeWriteSeqnos(replica).memory);
    // Check that the ack-seqno has been updated correctly
    EXPECT_EQ(memoryAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
    // Check that we committed and removed all SyncWrites
    EXPECT_EQ(0, monitor->public_getNumTracked());
    // Check that seqno-tracking is not lost after commit+remove
    EXPECT_EQ(3, monitor->public_getNodeWriteSeqnos(replica).memory);
    EXPECT_EQ(memoryAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
}

TEST_F(DurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanLastTracked_SparseSeqnos) {
    ASSERT_EQ(3, addSyncWrites({1, 3, 5} /*seqnos*/));
    ASSERT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).memory);

    int64_t memoryAckSeqno = 10;
    // Receive a seqno-ack greater than the last tracked seqno
    EXPECT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(
                      replica, memoryAckSeqno, 0 /*diskSeqno*/));
    // Check that the tracking has advanced to the last tracked seqno
    EXPECT_EQ(5, monitor->public_getNodeWriteSeqnos(replica).memory);
    // Check that the ack-seqno has been updated correctly
    EXPECT_EQ(memoryAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
    // Check that we committed and removed all SyncWrites
    EXPECT_EQ(0, monitor->public_getNumTracked());
    // Check that seqno-tracking is not lost after commit+remove
    EXPECT_EQ(5, monitor->public_getNodeWriteSeqnos(replica).memory);
    EXPECT_EQ(memoryAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
}

TEST_F(DurabilityMonitorTest,
       SeqnoAckReceived_MemorySeqnoSmallerThanDiskSeqno) {
    addSyncWrites({1} /*seqnos*/);
    try {
        monitor->seqnoAckReceived(replica, 0 /*memSeqno*/, 1 /*diskSeqno*/);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("memorySeqno < diskSeqno") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

// @todo: Refactor test suite and expand test cases
TEST_F(DurabilityMonitorTest, SeqnoAckReceived_PersistToMajority) {
    ASSERT_EQ(3,
              addSyncWrites({1, 3, 5} /*seqnos*/,
                            {cb::durability::Level::PersistToMajority,
                             0 /*timeout*/}));
    ASSERT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).disk);
    EXPECT_EQ(0, monitor->public_getNodeAckSeqnos(replica).disk);

    int64_t memAckSeqno = 10, diskAckSeqno = 10;

    // Receive a seqno-ack greater than the last tracked seqno
    EXPECT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(replica, memAckSeqno, diskAckSeqno));

    // Check that we have not committed as the active has not ack'ed the
    // persisted seqno
    EXPECT_EQ(3, monitor->public_getNumTracked());

    // Check that the tracking for Replica has been updated correctly
    EXPECT_EQ(5, monitor->public_getNodeWriteSeqnos(replica).disk);
    EXPECT_EQ(diskAckSeqno, monitor->public_getNodeAckSeqnos(replica).disk);

    // Check that the tracking for Active has not moved yet
    EXPECT_EQ(0, monitor->public_getNodeWriteSeqnos(active).disk);
    EXPECT_EQ(0, monitor->public_getNodeAckSeqnos(active).disk);

    // Simulate the Flusher that notifies the local DurabilityMonitor after
    // persistence
    vb->setPersistenceSeqno(diskAckSeqno);
    monitor->notifyLocalPersistence();

    // Check that we committed and removed all SyncWrites
    EXPECT_EQ(0, monitor->public_getNumTracked());

    // Check that the tracking for Active has been updated correctly
    EXPECT_EQ(5, monitor->public_getNodeWriteSeqnos(active).disk);
    EXPECT_EQ(diskAckSeqno, monitor->public_getNodeAckSeqnos(active).disk);
}

TEST_F(DurabilityMonitorTest, SetTopology_NotAnArray) {
    try {
        monitor->setReplicationTopology(nlohmann::json::object());
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("Topology is not an array") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SetTopology_Empty) {
    try {
        monitor->setReplicationTopology(nlohmann::json::array());
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("Topology is empty") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SetTopology_TooManyNodesInChain) {
    try {
        monitor->setReplicationTopology(nlohmann::json::array(
                {{"active", "replica1", "replica2", "replica3", "replica4"}}));
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("Too many nodes in chain") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SetTopology_NodeDuplicateIncChain) {
    try {
        monitor->setReplicationTopology(
                nlohmann::json::array({{"node1", "node1"}}));
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("Duplicate node") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

// @todo: Extend to disk-seqno
TEST_F(DurabilityMonitorTest, SeqnoAckReceived_MultipleReplica) {
    const std::string active = "active";
    const std::string replica1 = "replica1";
    const std::string replica2 = "replica2";
    const std::string replica3 = "replica3";

    ASSERT_NO_THROW(monitor->setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}})));
    ASSERT_EQ(4, monitor->public_getReplicationChainSize());

    addSyncWrite(1 /*seqno*/);

    // Active has implicitly ack'ed (SyncWrite added for tracking /after/ being
    // enqueued into the CheckpointManager)
    EXPECT_EQ(1, monitor->public_getNodeWriteSeqnos(active).memory);
    EXPECT_EQ(1, monitor->public_getNodeAckSeqnos(active).memory);

    // Nothing ack'ed yet for replica
    for (const auto& replica : {replica1, replica2, replica3}) {
        EXPECT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).memory);
        EXPECT_EQ(0, monitor->public_getNodeAckSeqnos(replica).memory);
    }
    // Nothing committed
    EXPECT_EQ(1, monitor->public_getNumTracked());

    // replica2 acks
    EXPECT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(
                      replica2, 1 /*memSeqno*/, 0 /*diskSeqno*/));
    EXPECT_EQ(1, monitor->public_getNodeWriteSeqnos(replica2).memory);
    EXPECT_EQ(1, monitor->public_getNodeAckSeqnos(replica2).memory);
    // Nothing committed yet
    EXPECT_EQ(1, monitor->public_getNumTracked());

    // replica3 acks
    EXPECT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(
                      replica3, 1 /*memSeqno*/, 0 /*diskSeqno*/));
    EXPECT_EQ(1, monitor->public_getNodeWriteSeqnos(replica3).memory);
    EXPECT_EQ(1, monitor->public_getNodeAckSeqnos(replica3).memory);
    // Requirements verified, committed
    EXPECT_EQ(0, monitor->public_getNumTracked());

    // replica1 has not ack'ed yet
    EXPECT_EQ(0, monitor->public_getNodeWriteSeqnos(replica1).memory);
    EXPECT_EQ(0, monitor->public_getNodeAckSeqnos(replica1).memory);
}

TEST_F(DurabilityMonitorTest, NeverExpireIfTimeoutNotSet) {
    ASSERT_NO_THROW(monitor->setReplicationTopology(
            nlohmann::json::array({{active, replica}})));
    ASSERT_EQ(2, monitor->public_getReplicationChainSize());

    // Note: Timeout=0 (i.e., no timeout) in default Durability Requirements
    ASSERT_EQ(1, addSyncWrites({1} /*seqno*/));
    EXPECT_EQ(1, monitor->public_getNumTracked());

    // Never expire, neither after 1 year !
    const auto year = std::chrono::hours(24 * 365);
    monitor->processTimeout(std::chrono::steady_clock::now() + year);

    // Not expired, still tracked
    EXPECT_EQ(1, monitor->public_getNumTracked());
}

TEST_F(DurabilityMonitorTest, ProcessTimeout) {
    ASSERT_NO_THROW(monitor->setReplicationTopology(
            nlohmann::json::array({{active, replica}})));
    ASSERT_EQ(2, monitor->public_getReplicationChainSize());

    auto checkMemoryTrack = [this](const std::string& node,
                                   int64_t expected) -> void {
        EXPECT_EQ(expected, monitor->public_getNodeWriteSeqnos(node).memory);
        EXPECT_EQ(expected, monitor->public_getNodeAckSeqnos(node).memory);
    };

    /*
     * 1 SyncWrite
     */

    const auto level = cb::durability::Level::Majority;

    ASSERT_EQ(1, addSyncWrites({1} /*seqno*/, {level, 1 /*timeout*/}));
    EXPECT_EQ(1, monitor->public_getNumTracked());
    checkMemoryTrack(active, 1);
    checkMemoryTrack(replica, 0);

    monitor->processTimeout(std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(1000));

    EXPECT_EQ(0, monitor->public_getNumTracked());
    checkMemoryTrack(active, 1);
    checkMemoryTrack(replica, 0);

    /*
     * Multiple SyncWrites, ordered by timeout
     */

    ASSERT_EQ(1, addSyncWrites({101} /*seqno*/, {level, 1 /*timeout*/}));
    ASSERT_EQ(1, addSyncWrites({102} /*seqno*/, {level, 10}));
    ASSERT_EQ(1, addSyncWrites({103} /*seqno*/, {level, 20}));
    EXPECT_EQ(3, monitor->public_getNumTracked());
    checkMemoryTrack(active, 103);
    checkMemoryTrack(replica, 0);

    monitor->processTimeout(std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(10000));

    EXPECT_EQ(0, monitor->public_getNumTracked());
    checkMemoryTrack(active, 103);
    checkMemoryTrack(replica, 0);

    /*
     * Multiple SyncWrites, not ordered by timeout
     */

    ASSERT_EQ(1, addSyncWrites({201} /*seqno*/, {level, 20 /*timeout*/}));
    ASSERT_EQ(1, addSyncWrites({202} /*seqno*/, {level, 1}));
    ASSERT_EQ(1, addSyncWrites({203} /*seqno*/, {level, 50000}));
    EXPECT_EQ(3, monitor->public_getNumTracked());
    checkMemoryTrack(active, 203);
    checkMemoryTrack(replica, 0);

    monitor->processTimeout(std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(10000));

    EXPECT_EQ(1, monitor->public_getNumTracked());
    const auto tracked = monitor->public_getTrackedSeqnos();
    EXPECT_TRUE(tracked.find(201) == tracked.end());
    EXPECT_TRUE(tracked.find(202) == tracked.end());
    EXPECT_TRUE(tracked.find(203) != tracked.end());
    checkMemoryTrack(active, 203);
    checkMemoryTrack(replica, 0);

    monitor->processTimeout(std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(100000));

    EXPECT_EQ(0, monitor->public_getNumTracked());
    checkMemoryTrack(active, 203);
    checkMemoryTrack(replica, 0);
}

TEST_F(DurabilityMonitorTest, MajorityAndPersistActive) {
    ASSERT_EQ(3,
              addSyncWrites({1, 3, 5} /*seqnos*/,
                            {cb::durability::Level::MajorityAndPersistOnMaster,
                             0 /*timeout*/}));
    ASSERT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).disk);
    EXPECT_EQ(0, monitor->public_getNodeAckSeqnos(replica).disk);

    int64_t memAckSeqno = 10, diskAckSeqno = 10;

    // Replica acks that (1) everything enqueued but (2) nothing persisted
    EXPECT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(replica, memAckSeqno, 0 /*diskSeqno*/));

    // The active has not ack'ed the persisted seqno, so nothing committed yet
    EXPECT_EQ(3, monitor->public_getNumTracked());

    // Check that the tracking for Replica has been updated correctly
    EXPECT_EQ(5, monitor->public_getNodeWriteSeqnos(replica).memory);
    EXPECT_EQ(diskAckSeqno, monitor->public_getNodeAckSeqnos(replica).memory);
    EXPECT_EQ(0, monitor->public_getNodeWriteSeqnos(replica).disk);
    EXPECT_EQ(0, monitor->public_getNodeAckSeqnos(replica).disk);

    // Check that the disk-tracking for Active has not moved yet
    EXPECT_EQ(0, monitor->public_getNodeWriteSeqnos(active).disk);
    EXPECT_EQ(0, monitor->public_getNodeAckSeqnos(active).disk);

    // Simulate the Flusher that notifies the local DurabilityMonitor after
    // persistence
    vb->setPersistenceSeqno(diskAckSeqno);
    monitor->notifyLocalPersistence();

    // All committed even if the Replica has not ack'ed the disk-seqno yet,
    // as Level::MajorityAndPersistOnMaste
    EXPECT_EQ(0, monitor->public_getNumTracked());

    // Check that the disk-tracking for Active has been updated correctly
    EXPECT_EQ(5, monitor->public_getNodeWriteSeqnos(active).disk);
    EXPECT_EQ(diskAckSeqno, monitor->public_getNodeAckSeqnos(active).disk);
}

/*
 * MB-33276: ReplicationChain iterators may stay invalid at Out-of-Order Commit.
 * That may lead to SEGFAULT as soon as an invalid iterator is processed.
 * Note that dereferencing/moving and invalid iterator is undefined behaviour,
 * so the way we crash is not deterministic.
 */
TEST_F(DurabilityMonitorTest, DontInvalidateIteratorsAtOutOfOrderCommit) {
    ASSERT_NO_THROW(
            addSyncWrite(1, {cb::durability::Level::PersistToMajority, 0}));
    ASSERT_NO_THROW(addSyncWrite(2, {cb::durability::Level::Majority, 0}));

    /*
     * End        s1(P)        s2(M)
     * ^^
     * A(m)/A(d)
     * ^^
     * R(m)/R(d)
     */

    ASSERT_EQ(2, monitor->public_getNumTracked());
    assertNodeMemTracking(active, 2 /*lastWriteSeqno*/, 2 /*lastAckSeqno*/);
    assertNodeDiskTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    assertNodeMemTracking(replica, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    assertNodeDiskTracking(replica, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);

    // Replica acks memSeqno:2
    ASSERT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(
                      replica, 2 /*memSeqno*/, 0 /*diskSeqno*/));

    /*
     * End        s1(P)        x
     * ^          ^
     * A(d)       A(m)
     * ^          ^
     * R(d)       R(m)
     */

    ASSERT_EQ(1, monitor->public_getNumTracked());
    assertNodeMemTracking(active, 2 /*lastWriteSeqno*/, 2 /*lastAckSeqno*/);
    assertNodeDiskTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    assertNodeMemTracking(replica, 2 /*lastWriteSeqno*/, 2 /*lastAckSeqno*/);
    assertNodeDiskTracking(replica, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);

    // Simulate the Flusher that notifies the local DurabilityMonitor after
    // persistence
    vb->setPersistenceSeqno(1);
    monitor->notifyLocalPersistence();

    ASSERT_EQ(1, monitor->public_getNumTracked());
    assertNodeMemTracking(active, 2 /*lastWriteSeqno*/, 2 /*lastAckSeqno*/);
    assertNodeDiskTracking(active, 1 /*lastWriteSeqno*/, 1 /*lastAckSeqno*/);
    assertNodeMemTracking(replica, 2 /*lastWriteSeqno*/, 2 /*lastAckSeqno*/);
    assertNodeDiskTracking(replica, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);

    // Replica acks diskSeqno:1
    ASSERT_EQ(ENGINE_SUCCESS,
              monitor->seqnoAckReceived(
                      replica, 2 /*memSeqno*/, 1 /*diskSeqno*/));

    /*
     * This is what happens before the fix (A(m) and R(m) stay invalid rather
     * than being repositioned to End):
     *
     * End        x        x
     * ^          ^
     * A(d)       A(m)
     * ^          ^
     * R(d)       R(m)
     */

    ASSERT_EQ(0, monitor->public_getNumTracked());
    assertNodeMemTracking(active, 2 /*lastWriteSeqno*/, 2 /*lastAckSeqno*/);
    assertNodeDiskTracking(active, 1 /*lastWriteSeqno*/, 1 /*lastAckSeqno*/);
    assertNodeMemTracking(replica, 2 /*lastWriteSeqno*/, 2 /*lastAckSeqno*/);
    assertNodeDiskTracking(replica, 1 /*lastWriteSeqno*/, 1 /*lastAckSeqno*/);

    // This crashes with SEGFAULT before the fix (caused by processing the
    // invalid A(m) iterator)
    addSyncWrite(10 /*seqno*/);
}
