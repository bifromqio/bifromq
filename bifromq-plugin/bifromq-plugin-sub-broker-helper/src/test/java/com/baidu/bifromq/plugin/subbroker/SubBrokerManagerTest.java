package com.baidu.bifromq.plugin.subbroker;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubBrokerManagerTest {
    private AutoCloseable closeable;
    @Mock
    private PluginManager plugin;

    @Mock
    private ISubBroker subBroker1;
    @Mock
    private IDeliverer deliverer1;
    @Mock
    private ISubBroker subBroker2;
    @Mock
    private IDeliverer deliverer2;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void subBrokerIdConflict() {
        when(subBroker1.id()).thenReturn(0);
        when(subBroker1.open(anyString())).thenReturn(deliverer1);
        when(subBroker2.id()).thenReturn(0);
        when(plugin.getExtensions(ISubBroker.class)).thenReturn(List.of(subBroker1, subBroker2));
        SubBrokerManager subBrokerManager = new SubBrokerManager(plugin);
        ISubBroker subBroker = subBrokerManager.get(0);
        IDeliverer deliverer = subBroker.open("Deliverer1");
        deliverer.close();
        verify(deliverer1, times(1)).close();
    }

    @Test
    public void noSubBroker() {
        when(subBroker1.id()).thenReturn(0);
        when(subBroker1.open(anyString())).thenReturn(deliverer1);
        when(plugin.getExtensions(ISubBroker.class)).thenReturn(List.of(subBroker1));
        SubBrokerManager subBrokerManager = new SubBrokerManager(plugin);
        ISubBroker subBroker = subBrokerManager.get(1);
        IDeliverer deliverer = subBroker.open("Deliverer1");
        TopicMessagePack msgPack = TopicMessagePack.newBuilder().build();
        SubInfo subInfo = SubInfo.newBuilder().build();
        Iterable<DeliveryPack> pack = List.of(new DeliveryPack(msgPack, List.of(subInfo)));
        Map<SubInfo, DeliveryResult> resultMap = deliverer.deliver(pack).join();
        assertEquals(resultMap.get(subInfo), DeliveryResult.NO_INBOX);
    }

    @Test
    public void stop() {
        when(subBroker1.id()).thenReturn(0);
        when(subBroker2.id()).thenReturn(1);
        when(plugin.getExtensions(ISubBroker.class)).thenReturn(List.of(subBroker1, subBroker2));
        SubBrokerManager subBrokerManager = new SubBrokerManager(plugin);
        subBrokerManager.stop();
        verify(subBroker1, times(1)).close();
        verify(subBroker2, times(1)).close();
    }
}
