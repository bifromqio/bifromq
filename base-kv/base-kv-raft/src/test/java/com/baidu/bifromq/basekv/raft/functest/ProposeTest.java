/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basekv.raft.functest;

import static com.baidu.bifromq.basekv.raft.exception.DropProposalException.OVERRIDDEN;
import static com.baidu.bifromq.basekv.raft.exception.DropProposalException.THROTTLED_BY_THRESHOLD;
import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.baidu.bifromq.basekv.raft.exception.CompactionException;
import com.baidu.bifromq.basekv.raft.functest.annotation.Cluster;
import com.baidu.bifromq.basekv.raft.functest.annotation.Config;
import com.baidu.bifromq.basekv.raft.functest.template.SharedRaftConfigTestTemplate;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class ProposeTest extends SharedRaftConfigTestTemplate {
    @Test
    public void testProposalOverridden1() {
        testProposalOverridden(true);
    }

    @Config(preVote = false)
    @Test
    public void testProposalOverridden2() {
        testProposalOverridden(true);
    }

    @Test
    public void testProposalOverridden3() {
        testProposalOverridden(false);
    }

    @Config(preVote = false)
    @Test
    public void testProposalOverridden4() {
        testProposalOverridden(false);
    }

    private void testProposalOverridden(boolean compaction) {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(leader, 1));
        group.propose(leader, copyFromUtf8("appCommand1"));
        group.propose(leader, copyFromUtf8("appCommand2"));
        group.propose(leader, copyFromUtf8("appCommand3"));
        assertTrue(group.awaitIndexCommitted(group.currentFollowers().get(0), 4));
        assertTrue(group.awaitIndexCommitted(group.currentFollowers().get(1), 4));

        log.info("Isolate {}", leader);
        group.isolate(leader);
        // following 3 entries are un-commit
        CompletableFuture<Void> propose5Future = group.propose(leader, copyFromUtf8("appCommand4")); // <- 5
        CompletableFuture<Void> propose6Future = group.propose(leader, copyFromUtf8("appCommand5")); // <- 6
        group.propose(leader, copyFromUtf8("appCommand6")); // <- 7
        group.propose(leader, copyFromUtf8("appCommand7")); // <- 8
        await().until(() -> group.currentLeader().isPresent() && !leader.equals(group.currentLeader().get()));
        group.await(200); // enough ticks to let old leader self step down
        String newLeader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(newLeader, 5));
        log.info("New leader {} elected", newLeader);
        assertNotEquals(leader, newLeader);
        // propose two more entries via new leader and wait for committed
        group.propose(newLeader, copyFromUtf8("appCommandA"));
        group.propose(newLeader, copyFromUtf8("appCommandB"));
        assertTrue(group.awaitIndexCommitted(newLeader, 7));
        // make a compaction and propose more
        if (compaction) {
            group.compact(newLeader, EMPTY, 7);
        }
        group.propose(newLeader, copyFromUtf8("appCommandC"));
        group.propose(newLeader, copyFromUtf8("appCommandD"));
        assertTrue(group.awaitIndexCommitted(newLeader, 9));
        // integrate old leader, and trigger install snapshot
        log.info("Integrate {}", leader);
        group.integrate(leader);
        assertTrue(group.awaitIndexCommitted(leader, 9));
        Assert.assertEquals(group.logEntries(newLeader, 8), group.logEntries(leader, 8));
        // the uncommitted proposal on old leader will be failed with Overridden exception
        try {
            propose5Future.get();
        } catch (Exception e) {
            assertEquals(OVERRIDDEN, e.getCause());
        }
        try {
            propose6Future.get();
        } catch (Exception e) {
            assertEquals(OVERRIDDEN, e.getCause());
        }
    }

    @Cluster(v = "V1")
    @Test
    public void testSingleNodePropose() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(leader, 1));

        group.propose(leader, copyFromUtf8("appCommand1"));
        assertTrue(group.awaitIndexCommitted(leader, 2));

        group.propose(leader, copyFromUtf8("appCommand2"));
        group.propose(leader, copyFromUtf8("appCommand3"));
        assertTrue(group.awaitIndexCommitted(leader, 4));

        List<LogEntry> entries = group.retrieveCommitted(leader, 2, -1);
        Optional<LogEntry> entry4 = group.entryAt(leader, 4);
        assertEquals(copyFromUtf8("appCommand3"), entry4.get().getData());
        assertEquals(3, entries.size());
        assertEquals(copyFromUtf8("appCommand1"), entries.get(0).getData());
        assertEquals(copyFromUtf8("appCommand2"), entries.get(1).getData());
        assertEquals(copyFromUtf8("appCommand3"), entries.get(2).getData());
    }

    @Test
    public void testProposeFromLeader() {
        String leader = group.currentLeader().get();
        assertTrue(group.awaitIndexCommitted(leader, 1));

        try {
            // propose from leader
            group.propose(leader, copyFromUtf8("appCommand1")).join();
            await().until(() -> 2 == group.commitIndex(leader));
            Assert.assertEquals(copyFromUtf8("appCommand1"),
                group.retrieveCommitted(leader, 2, -1).get(0).getData());
            group.propose(leader, copyFromUtf8("appCommand2")).join();
            await().until(() -> 3 == group.commitIndex(leader));
            Assert.assertEquals(copyFromUtf8("appCommand2"),
                group.retrieveCommitted(leader, 3, -1).get(0).getData());
            group.propose(leader, copyFromUtf8("appCommand3")).join();
            await().until(() -> 4 == group.commitIndex(leader));
            Assert.assertEquals(copyFromUtf8("appCommand3"),
                group.retrieveCommitted(leader, 4, -1).get(0).getData());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testProposeFromFollower() {
        assertTrue(group.awaitIndexCommitted("V1", 1));
        assertTrue(group.awaitIndexCommitted("V2", 1));
        assertTrue(group.awaitIndexCommitted("V3", 1));

        String follower = group.currentFollowers().get(0);
        try {
            group.propose(follower, copyFromUtf8("appCommand1")).join();
            Assert.assertEquals(2, group.commitIndex(group.currentLeader().get()));
            assertTrue(group.awaitIndexCommitted("V1", 2));
            assertTrue(group.awaitIndexCommitted("V2", 2));
            assertTrue(group.awaitIndexCommitted("V3", 2));
            Assert.assertEquals(copyFromUtf8("appCommand1"),
                group.retrieveCommitted(follower, 2, -1).get(0).getData());

            group.propose(follower, copyFromUtf8("appCommand2")).join();
            Assert.assertEquals(3, group.commitIndex(group.currentLeader().get()));
            group.propose(follower, copyFromUtf8("appCommand3")).join();
            Assert.assertEquals(4, group.commitIndex(group.currentLeader().get()));
            group.propose(follower, copyFromUtf8("appCommand4")).join();
            Assert.assertEquals(5, group.commitIndex(group.currentLeader().get()));
            assertTrue(group.awaitIndexCommitted("V1", 5));
            assertTrue(group.awaitIndexCommitted("V2", 5));
            assertTrue(group.awaitIndexCommitted("V3", 5));

            Assert.assertEquals(copyFromUtf8("appCommand4"),
                group.retrieveCommitted(follower, 5, -1).get(0).getData());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testProposeThrottled() {
        assertTrue(group.awaitIndexCommitted("V1", 1));
        assertTrue(group.awaitIndexCommitted("V2", 1));
        assertTrue(group.awaitIndexCommitted("V3", 1));
        String leader = group.currentLeader().get();
        log.info("Leader {} elected", leader);

        for (int i = 0; i < 1100; ++i) {
            group.propose(leader, copyFromUtf8(("appCommand-" + i)));
        }
        try {
            group.propose(leader, copyFromUtf8("appCommand-10")).join();
        } catch (Exception e) {
            assertSame(THROTTLED_BY_THRESHOLD, e.getCause());
        }
    }

    @Test
    public void testCompaction() {
        assertTrue(group.awaitIndexCommitted("V1", 1));
        assertTrue(group.awaitIndexCommitted("V2", 1));
        assertTrue(group.awaitIndexCommitted("V3", 1));
        String leader = group.currentLeader().get();
        log.info("Leader {} elected", leader);

        for (int i = 0; i < 10; ++i) {
            group.propose(leader, copyFromUtf8(("appCommand-" + i))).join();
        }
        group.compact(leader, ByteString.EMPTY, 5).join();
        try {
            group.entryAt(leader, 5);
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
        assertTrue(group.entryAt(leader, 6).isPresent());
        try {
            group.compact(leader, ByteString.EMPTY, 12).join();
        } catch (Exception e) {
            assertTrue(e.getCause() == CompactionException.STALE_SNAPSHOT);
        }
        try {
            group.compact(leader, ByteString.EMPTY, 4).join();
        } catch (Exception e) {
            assertTrue(e.getCause() == CompactionException.STALE_SNAPSHOT);
        }
    }
}
