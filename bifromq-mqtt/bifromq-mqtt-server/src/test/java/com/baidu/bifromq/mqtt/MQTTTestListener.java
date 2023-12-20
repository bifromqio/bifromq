/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

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
