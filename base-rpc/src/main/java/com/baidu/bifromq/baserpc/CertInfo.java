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

package com.baidu.bifromq.baserpc;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public final class CertInfo {
    private static final String KEY_PURPOSE_ID_SERVER_AUTH = "1.3.6.1.5.5.7.3.1";
    private static final String KEY_PURPOSE_ID_CLIENT_AUTH = "1.3.6.1.5.5.7.3.2";
    @NonNull
    public final String commonName;
    public final boolean serverAuth;
    public final boolean clientAuth;

    public static CertInfo parse(File certFile) {
        try (InputStream inStream = new FileInputStream(certFile)) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate cert = (X509Certificate) cf.generateCertificate(inStream);
            return parse(cert);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read cert file in X.509 format", e);
        }
    }

    public static CertInfo parse(X509Certificate cert) {
        try {
            String dn = cert.getSubjectX500Principal().getName();
            LdapName ldapDN = new LdapName(dn);
            String commonName = null;
            for (Rdn rdn : ldapDN.getRdns()) {
                switch (rdn.getType()) {
                    case "CN":
                        commonName = (String) rdn.getValue();
                        break;
                    default:
                        // do nothing
                }
            }
            List<String> extendedKeyUsage = cert.getExtendedKeyUsage();
            return new CertInfo(commonName,
                extendedKeyUsage.contains(KEY_PURPOSE_ID_SERVER_AUTH),
                extendedKeyUsage.contains(KEY_PURPOSE_ID_CLIENT_AUTH));
        } catch (Exception e) {
            throw new UnsupportedOperationException("Failed to read cert file in X.509 format", e);
        }
    }
}
