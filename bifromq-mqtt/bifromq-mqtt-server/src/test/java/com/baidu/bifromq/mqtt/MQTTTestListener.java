package com.baidu.bifromq.mqtt;

import lombok.extern.slf4j.Slf4j;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.lang.reflect.Method;

@Slf4j
public class MQTTTestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        String className = result.getInstance().getClass().getName();
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        log.info("Test case[{}.{}] start", className, method.getName());
    }
}
