package com.baidu.bifromq.basekv.store;

import org.testng.ITestListener;
import org.testng.ITestResult;

import java.lang.reflect.Method;

public class KVRangeStoreTestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        if (result.getInstance() instanceof KVRangeStoreClusterTestTemplate inst) {
            inst.createClusterByAnnotation(method);
        }
    }
}
