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

package com.baidu.bifromq.basecrdt.store;

import static com.baidu.bifromq.basecrdt.core.util.Log.info;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.baidu.bifromq.basecrdt.core.api.AWORSetOperation;
import com.baidu.bifromq.basecrdt.core.api.CCounterOperation;
import com.baidu.bifromq.basecrdt.core.api.CRDTURI;
import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IAWORSet;
import com.baidu.bifromq.basecrdt.core.api.ICCounter;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.MVRegOperation;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.store.annotation.StoreCfg;
import com.baidu.bifromq.basecrdt.store.annotation.StoreCfgs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class CRDTStoreTest extends CRDTStoreTestTemplate {

    @StoreCfgs(stores = {@StoreCfg(id = "s1")})
    @Test
    public void testHosting() {
        ICRDTStore store = storeMgr.getStore("s1");
        String awseturi = CRDTURI.toURI(CausalCRDTType.aworset, "set");
        Replica awset = store.host(awseturi);
        ByteString id = awset.getId();
        Set<Replica> hosted = Sets.newHashSet(store.hosting());
        assertEquals(Sets.newHashSet(awset), hosted);
        assertTrue(store.get(awseturi).isPresent());
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test
    public void testAntiEntropy() {
        String setId = CRDTURI.toURI(CausalCRDTType.aworset, "set");
        Replica setR1 = storeMgr.host("s1", setId);
        Replica setR2 = storeMgr.host("s2", setId);
        Replica setR3 = storeMgr.host("s3", setId);
        IAWORSet set1 = (IAWORSet) storeMgr.getStore("s1").get(setId).get();
        IAWORSet set2 = (IAWORSet) storeMgr.getStore("s2").get(setId).get();
        IAWORSet set3 = (IAWORSet) storeMgr.getStore("s3").get(setId).get();
        ByteString value1 = copyFromUtf8("Value1");
        ByteString value2 = copyFromUtf8("Value2");
        ByteString value3 = copyFromUtf8("Value3");

        // build network
        storeMgr.join("s1", setId, setR1.getId(), setR1.getId(), setR2.getId(), setR3.getId());
        storeMgr.join("s2", setId, setR2.getId(), setR1.getId(), setR2.getId(), setR3.getId());
        storeMgr.join("s3", setId, setR3.getId(), setR1.getId(), setR2.getId(), setR3.getId());

        set1.execute(AWORSetOperation.add(value1)).join();
        awaitUntilTrue(() -> set1.contains(value1), 5000);
        awaitUntilTrue(() -> set2.contains(value1), 5000);
        awaitUntilTrue(() -> set3.contains(value1), 5000);

        set2.execute(AWORSetOperation.add(value2)).join();
        awaitUntilTrue(() -> set1.contains(value2), 5000);
        awaitUntilTrue(() -> set2.contains(value2), 5000);
        awaitUntilTrue(() -> set3.contains(value2), 5000);

        set3.execute(AWORSetOperation.add(value3)).join();
        awaitUntilTrue(() -> set1.contains(value3), 5000);
        awaitUntilTrue(() -> set2.contains(value3), 5000);
        awaitUntilTrue(() -> set3.contains(value3), 5000);

        set1.execute(AWORSetOperation.clear()).join();
        awaitUntilTrue(() -> set1.isEmpty(), 5000);
        awaitUntilTrue(() -> set2.isEmpty(), 5000);
        awaitUntilTrue(() -> set3.isEmpty(), 5000);
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s0"),
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
        @StoreCfg(id = "s4"),
        @StoreCfg(id = "s5"),
        @StoreCfg(id = "s6"),
        @StoreCfg(id = "s7"),
        @StoreCfg(id = "s8"),
        @StoreCfg(id = "s9"),
        @StoreCfg(id = "s10"),
        @StoreCfg(id = "s11"),
        @StoreCfg(id = "s12"),
        @StoreCfg(id = "s13"),
        @StoreCfg(id = "s14"),
        @StoreCfg(id = "s15"),
        @StoreCfg(id = "s16"),
        @StoreCfg(id = "s17"),
        @StoreCfg(id = "s18"),
        @StoreCfg(id = "s19"),
    })
    @Test
    public void testMassiveNestedCounters() throws InterruptedException {
        int size = storeMgr.stores().size();
        int countersPerMap = 100;
        int countPerCounter = 100;
        CountDownLatch latch = new CountDownLatch(countPerCounter * countersPerMap * size);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        String counterMap = CRDTURI.toURI(CausalCRDTType.ormap, "counter");
        ByteString[] replicaIds = new ByteString[size];
        long[][] reads = new long[size][countersPerMap];
        Map<ByteString, List<ICCounter>> ctrMap = Maps.newHashMap();
        AtomicBoolean inverseHappened = new AtomicBoolean();
        for (int i = 0; i < size; i++) {
            Replica ctrMapR = storeMgr.host("s" + i, counterMap);
            replicaIds[i] = ctrMapR.getId();
            reads[i] = new long[countersPerMap];
            IORMap map = (IORMap) storeMgr.getStore("s" + i).get(counterMap).get();
            ctrMap.computeIfAbsent(ctrMapR.getId(), k -> Lists.newArrayList());
            for (int c = 0; c < countersPerMap; c++) {
                ctrMap.get(ctrMapR.getId()).add(map.getCCounter(ByteString.copyFromUtf8("c_" + c)));
            }
        }
        for (int i = 0; i < size; i++) {
            storeMgr.join("s" + i, counterMap, replicaIds[i], replicaIds);
        }
        long start = System.nanoTime();
        for (int i = 0; i < size; i++) {
            int j = i;
            class CounterAdder {
                final int mapIdx;
                final int ctrIdx;
                final ICCounter ctr;
                final long delay;
                volatile int count;

                CounterAdder(int mapIdx, int ctrIdx, ICCounter ctr, long delay) {
                    this.mapIdx = mapIdx;
                    this.ctrIdx = ctrIdx;
                    this.ctr = ctr;
                    this.delay = delay;
                }

                void start() {
                    if (count++ < countPerCounter) {
                        ctr.execute(CCounterOperation.add(1));
                        long read = reads[mapIdx][ctrIdx];
                        reads[mapIdx][ctrIdx] = ctr.read();
                        if (read > reads[mapIdx][ctrIdx]) {
                            inverseHappened.set(true);
                        }
                        latch.countDown();
                        executor.schedule(this::start, ThreadLocalRandom.current().nextLong(delay),
                            TimeUnit.MILLISECONDS);
                    }
                }
            }
            for (int c = 0; c < ctrMap.get(replicaIds[j]).size(); c++) {
                ICCounter cctr = ctrMap.get(replicaIds[j]).get(c);
                new CounterAdder(i, c, cctr, 10).start();
            }
        }

        latch.await();
        log.info("Finish counting in {} ms", Duration.ofNanos(System.nanoTime() - start).toMillis());
        awaitUntilTrue(() -> {
            boolean pass = true;
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < ctrMap.get(replicaIds[i]).size(); j++) {
                    ICCounter ctr = ctrMap.get(replicaIds[i]).get(j);
                    long read = reads[i][j];
                    reads[i][j] = ctr.read();
                    if (read > reads[i][j]) {
                        inverseHappened.set(true);
                    }
                    if (ctr.read() != countPerCounter * size) {
                        pass = false;
                    }
                }
            }
            return pass;
        }, 200000);
        assertFalse(inverseHappened.get());
        log.info("Pass test in {} ms", Duration.ofNanos(System.nanoTime() - start).toMillis());
//        Thread.sleep(10000);
        executor.shutdownNow();
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s0", inflationInterval = 10),
        @StoreCfg(id = "s1", inflationInterval = 10),
        @StoreCfg(id = "s2", packetRandom = true),
        @StoreCfg(id = "s3", inflationInterval = 10),
        @StoreCfg(id = "s4", packetLossPercent = 0.1),
        @StoreCfg(id = "s5", packetDelayTime = 100),
        @StoreCfg(id = "s6"),
        @StoreCfg(id = "s7"),
        @StoreCfg(id = "s8"),
        @StoreCfg(id = "s9")
    })
    @Test
    public void testCCountersWithPacketLossAndReorder() throws InterruptedException {
        ccounterConcurrentWritingTest(5000);
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "ss0", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss1", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss2", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss3", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss4", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss5", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss6", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss7", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss8", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss9", inflationInterval = 10, packetRandom = false)
    })
    @Test
    public void testCCounters() throws InterruptedException {
        ccounterConcurrentWritingTest(10000);
    }

    // This method is used to simulate the cluster locally
    private void ccounterConcurrentWritingTest(int countPerCounter) throws InterruptedException {
        List<String> storeIds = storeMgr.stores();
        CountDownLatch latch = new CountDownLatch(countPerCounter * storeIds.size());
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        String ctrId = CRDTURI.toURI(CausalCRDTType.cctr, "counter");
        ByteString[] replicaIds = new ByteString[storeIds.size()];
        ICCounter[] counters = new ICCounter[storeIds.size()];
        AtomicInteger[] counts = new AtomicInteger[storeIds.size()];
        AtomicLong[] lastReads = new AtomicLong[storeIds.size()];
        for (int i = 0; i < storeIds.size(); i++) {
            Replica ctrR = storeMgr.host(storeIds.get(i), ctrId);
            replicaIds[i] = ctrR.getId();
            counters[i] = (ICCounter) storeMgr.getStore(storeIds.get(i)).get(ctrId).get();
            counts[i] = new AtomicInteger();
            lastReads[i] = new AtomicLong();
        }
        for (int i = 0; i < storeIds.size(); i++) {
            storeMgr.join(storeIds.get(i), ctrId, replicaIds[i], replicaIds);
        }
        AtomicBoolean inverse = new AtomicBoolean(false);
        for (int i = 0; i < storeIds.size(); i++) {
            int j = i;
            executor.scheduleAtFixedRate(() -> {
                if (counts[j].getAndIncrement() < countPerCounter) {
                    counters[j].execute(CCounterOperation.add(1));
                    latch.countDown();
                    long r = lastReads[j].get();
                    lastReads[j].set(counters[j].read());
                    if (r > lastReads[j].get()) {
                        inverse.set(true);
                        info(log, "Counter {}: last={}, now={}, diff={}, inverse={}",
                            counters[j].id(), r, lastReads[j].get(), lastReads[j].get() - r, r > lastReads[j].get());
                    }
                }
            }, 0, 1, TimeUnit.MILLISECONDS);
        }

        latch.await();
        log.info("Finish counting");
        awaitUntilTrue(() -> {
            boolean pass = true;
            for (int i = 0; i < storeIds.size(); i++) {
                if (counters[i].read() != countPerCounter * storeIds.size()) {
                    info(log, "Counter[{}] reads:{}", counters[i].id(), counters[i].read());
                    pass = false;
                }
            }
            return pass;
        }, 150000);
        Assert.assertFalse(inverse.get());
        executor.shutdownNow();
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3")
    })
    @Test
    public void testNewReplicaJoin() {
        String setId = CRDTURI.toURI(CausalCRDTType.aworset, "set");
        Replica setR1 = storeMgr.host("s1", setId);
        Replica setR2 = storeMgr.host("s2", setId);
        Replica setR3 = storeMgr.host("s3", setId);
        IAWORSet set1 = (IAWORSet) storeMgr.getStore("s1").get(setId).get();
        IAWORSet set2 = (IAWORSet) storeMgr.getStore("s2").get(setId).get();
        IAWORSet set3 = (IAWORSet) storeMgr.getStore("s3").get(setId).get();
        ByteString value1 = copyFromUtf8("Value1");
        ByteString value2 = copyFromUtf8("Value2");
        ByteString value3 = copyFromUtf8("Value3");

        // build two members network
        storeMgr.join("s1", setId, setR1.getId(), setR1.getId(), setR2.getId());
        storeMgr.join("s2", setId, setR2.getId(), setR1.getId(), setR2.getId());

        set1.execute(AWORSetOperation.add(value1)).join();
        awaitUntilTrue(() -> set1.contains(value1));
        awaitUntilTrue(() -> set2.contains(value1));

        set2.execute(AWORSetOperation.add(value2)).join();
        awaitUntilTrue(() -> set1.contains(value2));
        awaitUntilTrue(() -> set2.contains(value2));

        set3.execute(AWORSetOperation.add(value3)).join();
        awaitUntilTrue(() -> set3.contains(value3));


        // build three members network
        storeMgr.join("s1", setId, setR1.getId(), setR1.getId(), setR2.getId(), setR3.getId());
        storeMgr.join("s2", setId, setR2.getId(), setR1.getId(), setR2.getId(), setR3.getId());
        storeMgr.join("s3", setId, setR3.getId(), setR1.getId(), setR2.getId(), setR3.getId());

        awaitUntilTrue(() -> set1.contains(value3));
        awaitUntilTrue(() -> set2.contains(value3));

        awaitUntilTrue(() -> set3.contains(value1));
        awaitUntilTrue(() -> set3.contains(value2));
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test
    public void testMemberLeave() throws InterruptedException {
        String ctrId = CRDTURI.toURI(CausalCRDTType.cctr, "cctr");
        Replica ctrR1 = storeMgr.host("s1", ctrId);
        Replica ctrR2 = storeMgr.host("s2", ctrId);
        Replica ctrR3 = storeMgr.host("s3", ctrId);
        ICCounter c1 = (ICCounter) storeMgr.getStore("s1").get(ctrId).get();
        ICCounter c2 = (ICCounter) storeMgr.getStore("s2").get(ctrId).get();
        ICCounter c3 = (ICCounter) storeMgr.getStore("s3").get(ctrId).get();

        storeMgr.join("s1", ctrId, ctrR1.getId(), ctrR1.getId(), ctrR2.getId(), ctrR3.getId());
        storeMgr.join("s2", ctrId, ctrR2.getId(), ctrR1.getId(), ctrR2.getId(), ctrR3.getId());
        storeMgr.join("s3", ctrId, ctrR3.getId(), ctrR1.getId(), ctrR2.getId(), ctrR3.getId());

        c1.execute(CCounterOperation.add(10)).join();
        c2.execute(CCounterOperation.add(10)).join();
        c3.execute(CCounterOperation.add(10)).join();
        awaitUntilTrue(() -> c1.read() == 30);
        awaitUntilTrue(() -> c2.read() == 30);
        awaitUntilTrue(() -> c3.read() == 30);

        // s3 stopped gracefully
        storeMgr.stopStore("s3");
        // sleep for a while
        Thread.sleep(1000);

        // reset the cluster landscape of s1, s2
        storeMgr.join("s1", ctrId, ctrR1.getId(), ctrR1.getId(), ctrR2.getId());
        storeMgr.join("s2", ctrId, ctrR2.getId(), ctrR1.getId(), ctrR2.getId());

        // partition the state of s3 as well
//        storeMgr.partition("s1", ctrId, ctrR3.getId()).join();
        c1.execute(CCounterOperation.zeroOut(ctrR3.getId())).join();
//        storeMgr.partition("s2", ctrId, ctrR3.getId()).join();
        c2.execute(CCounterOperation.zeroOut(ctrR3.getId())).join();
        awaitUntilTrue(() -> c1.read() == 20);
        awaitUntilTrue(() -> c2.read() == 20);
    }


    @StoreCfgs(stores = {
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test
    public void testAutoHealWhenFalsePositiveNetworkPartition() throws InterruptedException {
        String clusterId = CRDTURI.toURI(CausalCRDTType.ormap, "ormap");
        Replica clusterR1 = storeMgr.host("s1", clusterId);
        Replica clusterR2 = storeMgr.host("s2", clusterId);
        Replica clusterR3 = storeMgr.host("s3", clusterId);
        IORMap c1 = (IORMap) storeMgr.getStore("s1").get(clusterId).get();
        IORMap c2 = (IORMap) storeMgr.getStore("s2").get(clusterId).get();
        IORMap c3 = (IORMap) storeMgr.getStore("s3").get(clusterId).get();
        ByteString c1Addr = copyFromUtf8("ipaddr1");
        ByteString c2Addr = copyFromUtf8("ipaddr2");
        ByteString c3Addr = copyFromUtf8("ipaddr3");

        storeMgr.join("s1", clusterId, clusterR1.getId(), clusterR1.getId(), clusterR2.getId(), clusterR3.getId());
        storeMgr.join("s2", clusterId, clusterR2.getId(), clusterR1.getId(), clusterR2.getId(), clusterR3.getId());
        storeMgr.join("s3", clusterId, clusterR3.getId(), clusterR1.getId(), clusterR2.getId(), clusterR3.getId());

        c1.execute(ORMapOperation.update(clusterR1.getId()).with(MVRegOperation.write(c1Addr))).join();
        c2.execute(ORMapOperation.update(clusterR2.getId()).with(MVRegOperation.write(c2Addr))).join();
        c3.execute(ORMapOperation.update(clusterR3.getId()).with(MVRegOperation.write(c3Addr))).join();
        awaitUntilTrue(() -> Sets.newHashSet(c1.keys()).size() == 3);
        awaitUntilTrue(() -> Sets.newHashSet(c2.keys()).size() == 3);
        awaitUntilTrue(() -> Sets.newHashSet(c3.keys()).size() == 3);

        // sleep for a while to let anti-entropy between s1 and s2 become quiescent,
        // otherwise partition operation may not succeed within s1, s2
        Thread.sleep(1000);
        // simulate s3 is detected unreachable wrongly from s1, s2's failure detector,
        // partition s3's state from their respective local state proactively
//        storeMgr.partition("s1", clusterId, clusterR3.getId()).join();
        c1.execute(ORMapOperation.remove(clusterR3.getId()).of(CausalCRDTType.mvreg)).join();
//        storeMgr.partition("s2", clusterId, clusterR3.getId()).join();
        c2.execute(ORMapOperation.remove(clusterR3.getId()).of(CausalCRDTType.mvreg)).join();
        awaitUntilTrue(() -> Sets.newHashSet(c1.keys()).size() == 2);
        awaitUntilTrue(() -> Sets.newHashSet(c2.keys()).size() == 2);

        // and add s3 to local-split-brain resolve list and initialize split-brain resolving process
        storeMgr.join("s1", clusterId, clusterR1.getId(), clusterR1.getId(), clusterR2.getId());
        storeMgr.join("s1", clusterId, clusterR1.getId(), clusterR1.getId(), clusterR2.getId(), clusterR3.getId());

        storeMgr.join("s2", clusterId, clusterR2.getId(), clusterR1.getId(), clusterR2.getId());
        storeMgr.join("s2", clusterId, clusterR2.getId(), clusterR1.getId(), clusterR2.getId(), clusterR3.getId());
        // changes to s1, s2 will propagate to s3
        awaitUntilTrue(() -> Sets.newHashSet(c3.keys()).size() == 2);

        // s3 acknowledged it's been detected as a failed member in the current cluster, so it refutes
        c3.execute(ORMapOperation.update(clusterR3.getId()).with(MVRegOperation.write(c3Addr))).join();

        // by either forcing sync with detecting members or
//        storeMgr.sync("s3", clusterId, clusterR1.getId());
//        storeMgr.sync("s3", clusterId, clusterR2.getId());

        // 1) leave the cluster(by joining a single member cluster)
//        storeMgr.join("s3", clusterId, clusterR3.getId(), clusterR3.getId());
        // 2) join to the original cluster
//        storeMgr.join("s3", clusterId, clusterR3.getId(), clusterR1.getId(), clusterR2.getId(), clusterR3.getId());
        // NOTE:
        // state from s3 is recovered in s1, s2, split brain healed
        awaitUntilTrue(() -> Sets.newHashSet(c1.keys()).size() == 3);
        awaitUntilTrue(() -> Sets.newHashSet(c2.keys()).size() == 3);
        awaitUntilTrue(() -> Sets.newHashSet(c3.keys()).size() == 3);
    }
}
