package com.baidu.bifromq.basecluster;

import org.testng.ITestListener;
import org.testng.ITestResult;

import java.lang.reflect.Method;

public class AgentHostTestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        if (result.getInstance() instanceof AgentTestTemplate inst) {
            inst.createClusterByAnnotation(method);
        }
    }
}
