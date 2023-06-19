package com.baidu.bifromq.basecrdt.store;

import org.testng.ITestListener;
import org.testng.ITestResult;

import java.lang.reflect.Method;

public class CRDTStoreTestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        if (result.getInstance() instanceof CRDTStoreTestTemplate inst) {
            inst.createClusterByAnnotation(method);
        }
    }
}
