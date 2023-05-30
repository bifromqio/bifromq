/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.starter.utils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceUtil {
    /**
     * Get file using default classloader
     *
     * @param pathToResourceFile
     * @return
     */
    public static File getFile(@NonNull String pathToResourceFile) {
        URL url = ResourceUtil.class.getClassLoader().getResource(pathToResourceFile);
        if (url != null) {
            log.trace("File found in resources: {}", pathToResourceFile);
            try {
                URI uri = url.toURI();
                return new File(uri);
            } catch (URISyntaxException e) {
                log.error("Failed to parse url:{}", url, e);
            }
        }
        return null;
    }

    /**
     * Get the file from relativePath, if env variable(the 2nd argument) is specified, will try to load the file in
     * corresponding directory first, then load it via same class loader as a resource, return null if not found
     *
     * @param relativePathToFile the relative path of the file
     * @param sysPropOfDir       it's the env var specifies the directory to look for first
     * @return
     */
    public static File getFile(@NonNull String relativePathToFile, String sysPropOfDir) {
        // First try to get from config directory.
        if (sysPropOfDir != null && !"".equals(sysPropOfDir)) {
            String dir = System.getProperty(sysPropOfDir);
            if (dir != null) {
                File file = new File(dir, relativePathToFile);
                if (file.exists()) {
                    log.trace("File found in path: {}", file.getAbsolutePath());
                    return file;
                }
            }
        }
        log.warn("No dir or file found from sysPropOfDir:{}, fall back to classpath.", sysPropOfDir);
        return getFile(relativePathToFile);
    }
}
