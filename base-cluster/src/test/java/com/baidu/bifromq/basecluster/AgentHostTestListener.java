package com.baidu.bifromq.basecluster;

import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.annotations.Listeners;

import java.lang.reflect.Method;
import java.util.Arrays;

public class AgentHostTestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        Listeners listeners = result.getInstance().getClass().getAnnotation(Listeners.class);
        if (listeners != null && Arrays.asList(listeners.value()).contains(AgentHostTestListener.class)) {
            if (result.getInstance() instanceof AgentTestTemplate inst) {
                Method method = result.getMethod().getConstructorOrMethod().getMethod();
                inst.createClusterByAnnotation(method);
            }
        }
    }
}
