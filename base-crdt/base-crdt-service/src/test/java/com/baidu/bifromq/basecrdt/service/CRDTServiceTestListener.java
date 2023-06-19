package com.baidu.bifromq.basecrdt.service;

import org.testng.ITestListener;
import org.testng.ITestResult;

import java.lang.reflect.Method;

public class CRDTServiceTestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        if (result.getInstance() instanceof CRDTServiceTestTemplate inst) {
            inst.createClusterByAnnotation(method);
        }
    }
}
