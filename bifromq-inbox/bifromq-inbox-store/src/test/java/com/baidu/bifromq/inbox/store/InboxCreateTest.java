package com.baidu.bifromq.inbox.store;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.CreateRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.UpdateRequest;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MQTT3ClientInfo;
import com.google.protobuf.ByteString;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class InboxCreateTest {
    private KVRangeId id;
    private Map<ByteString, ByteString> map = new HashMap<>();
    @Mock
    private IKVReader reader;
    @Mock
    private IKVIterator kvIterator;
    private IKVWriter writer = new TestWriter(map);
    private Supplier<IKVRangeReader> rangeReaderProvider = () -> null;
    private IEventCollector eventCollector = event -> {};
    private String trafficId = "trafficId";
    private String inboxId = "inboxId";
    private String scopedInboxIdUtf8 = scopedInboxId(trafficId, inboxId).toStringUtf8();
    private ClientInfo clientInfo = ClientInfo.newBuilder()
            .setTrafficId(trafficId)
            .setUserId("testUser")
            .setMqtt3ClientInfo(MQTT3ClientInfo.newBuilder()
                    .setClientId("clientId")
                    .setPort(8888)
                    .setIp("127.0.0.1")
                    .build())
            .build();
    private Clock clock = Clock.systemUTC();
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(reader.iterator()).thenReturn(kvIterator);
        id = KVRangeIdUtil.generate();
    }

    @AfterMethod
    public void teardown() throws Exception {
        map.clear();
        closeable.close();
    }

    @Test
    public void testCreateNewInbox() {
        CreateRequest createRequest = CreateRequest.newBuilder()
                .putInboxes(scopedInboxIdUtf8, CreateParams.newBuilder()
                        .setClient(clientInfo)
                        .build())
                .build();
        UpdateRequest updateRequest = UpdateRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setCreateInbox(createRequest)
                .build();
        InboxServiceRWCoProcInput coProcInput = InboxServiceRWCoProcInput.newBuilder()
                .setRequest(updateRequest)
                .build();

        when(reader.get(any())).thenReturn(Optional.empty());

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30));
        coProc.mutate(coProcInput.toByteString(), reader, writer);

        try {
            InboxMetadata inboxMetadata = InboxMetadata.parseFrom(map.get(ByteString.copyFromUtf8(scopedInboxIdUtf8)));
            assertEquals(0, inboxMetadata.getQos0NextSeq());
            assertEquals(0, inboxMetadata.getQos1NextSeq());
            assertEquals(0, inboxMetadata.getQos2NextSeq());
            assertEquals(clientInfo, inboxMetadata.getClient());
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testCreateExpiredInbox() {
        CreateRequest createRequest = CreateRequest.newBuilder()
                .putInboxes(scopedInboxIdUtf8, CreateParams.newBuilder()
                        .setClient(clientInfo)
                        .build())
                .build();
        UpdateRequest updateRequest = UpdateRequest.newBuilder()
                .setReqId(System.nanoTime())
                .setCreateInbox(createRequest)
                .build();
        InboxServiceRWCoProcInput coProcInput = InboxServiceRWCoProcInput.newBuilder()
                .setRequest(updateRequest)
                .build();

        when(reader.get(any())).thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis() - 30 * 1000)
                        .setQos0NextSeq(1)
                        .setQos1NextSeq(1)
                        .setQos2NextSeq(1)
                        .setExpireSeconds(1)
                .build().toByteString()));
        doNothing().when(kvIterator).seek(any());
        when(kvIterator.isValid()).thenReturn(false);

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                Clock.systemUTC(), Duration.ofMinutes(30));
        coProc.mutate(coProcInput.toByteString(), reader, writer);

        try {
            InboxMetadata inboxMetadata = InboxMetadata.parseFrom(map.get(ByteString.copyFromUtf8(scopedInboxIdUtf8)));
            assertEquals(0, inboxMetadata.getQos0NextSeq());
            assertEquals(0, inboxMetadata.getQos1NextSeq());
            assertEquals(0, inboxMetadata.getQos2NextSeq());
            assertEquals(clientInfo, inboxMetadata.getClient());
        }catch (Exception exception) {
            fail();
        }
    }

    class TestWriter implements IKVWriter {
        private Map<ByteString, ByteString> map;

        TestWriter(Map<ByteString, ByteString> map) {
            this.map = map;
        }
        @Override
        public void delete(ByteString key) {
            map.remove(key);
        }

        @Override
        public void deleteRange(Range range) {

        }

        @Override
        public void insert(ByteString key, ByteString value) {
            map.put(key, value);
        }

        @Override
        public void put(ByteString key, ByteString value) {
            map.put(key, value);
        }
    }
}
