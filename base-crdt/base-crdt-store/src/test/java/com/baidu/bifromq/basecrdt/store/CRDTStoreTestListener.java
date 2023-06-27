package com.baidu.bifromq.basecrdt.store;

import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.annotations.Listeners;

import java.lang.reflect.Method;
import java.util.Arrays;

public class CRDTStoreTestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        Listeners listeners = result.getInstance().getClass().getAnnotation(Listeners.class);
        if (listeners != null && Arrays.asList(listeners.value()).contains(CRDTStoreTestListener.class)) {
            if (result.getInstance() instanceof CRDTStoreTestTemplate inst) {
                Method method = result.getMethod().getConstructorOrMethod().getMethod();
                inst.createClusterByAnnotation(method);
            }
        }
    }
}
