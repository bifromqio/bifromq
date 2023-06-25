package com.baidu.bifromq.basekv.raft.functest.template;

import org.testng.ITestListener;
import org.testng.ITestResult;

import java.lang.reflect.Method;

public class RaftGroupTestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        if (result.getInstance() instanceof RaftGroupTestTemplate inst) {
            inst.createClusterByAnnotation(method);
        }
    }
}
