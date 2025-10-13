package com.emqx.flink.connector;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyFactory;
// import java.security.Security;
// import java.util.Base64;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.bouncycastle.asn1.DERNull;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.pkcs.RSAPrivateKey;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
// import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SSLUtil.class);

    private static final String KEY_STORE_PASSWORD = "emqx";
    
    // public static boolean isKeyPkcs1(File keyPem) throws Exception {
    //     return checkKeyPemType(keyPem, "RSA PRIVATE KEY");
    // }

    // public static boolean isKeyPkcs8(File keyPem) throws Exception {
    //     return checkKeyPemType(keyPem, "PRIVATE KEY");
    // }

    // private static boolean checkKeyPemType(File keyPemFile, String type) throws Exception {
    //     PemObject pem;
    //     try (PemReader pr = new PemReader(new FileReader(keyPemFile))) {
    //         pem = pr.readPemObject();
    //         return pem.getType().equals(type);
    //     } catch (Exception e) {
    //         throw e;
    //     }
    // }

    private static byte[] pkcs1ToPkcs8(PemObject pem) throws Exception {
        RSAPrivateKey pkcs1Key = RSAPrivateKey.getInstance(pem.getContent());
        try {
            PrivateKeyInfo pkcs8Info = new PrivateKeyInfo(
            new AlgorithmIdentifier(PKCSObjectIdentifiers.rsaEncryption, DERNull.INSTANCE),
            pkcs1Key.toASN1Primitive()
        );
        byte[] pkcs8Encoded = pkcs8Info.getEncoded();

        PemObject pkcs8Pem = new PemObject("PRIVATE KEY", pkcs8Encoded);
        return pkcs8Pem.getContent();
        } catch (Exception e) {
            throw e;
        }
        // // parse PKCS#1 by BouncyCastle
        // String pem = new String(keyBytes);
        // pem = pem
        //     .replace("-----BEGIN RSA PRIVATE KEY-----","")
        //     .replace("-----END RSA PRIVATE KEY-----", "")
        //     .replaceAll("\\s+", "");

        // byte[] decoded = Base64.getDecoder().decode(pem);
        
        // Security.addProvider(new BouncyCastleProvider());
        

        // // convert to PKCS#8

        
        // return null;
    }

    public static PrivateKey getPrivateKey(File keyPemFile) {
        PemObject pem;
        byte[] pkcs8Key = null;
        try (PemReader pr = new PemReader(new FileReader(keyPemFile))) {
            pem = pr.readPemObject();
            if (pem.getType().equals("PRIVATE KEY")) {
                pkcs8Key = pem.getContent();
            } else if (pem.getType().equals("RSA PRIVATE KEY")) {
                pkcs8Key = pkcs1ToPkcs8(pem);
            } else {
                LOG.error("Failed to handle keys other than PKCS#1 and PKCS#8");
            }
            if (pkcs8Key != null) {
                KeyFactory kf = KeyFactory.getInstance("RSA", "BC");
                PrivateKey key = kf.generatePrivate(new PKCS8EncodedKeySpec(pkcs8Key));
                return key;
            }
        } catch (Exception e) {
            LOG.error("Error occurred when getting PKCS#8 key for connector", e);
            return null;
        }
        return null;
    }

    public static List<Certificate> getCertChain(File certPem) {
        List<Certificate> chain = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(certPem)))) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
                if (line.contains("END CERTIFICATE")) {
                    byte[] crtBytes = sb.toString().getBytes();
                    chain.add(cf.generateCertificate(new ByteArrayInputStream(crtBytes)));
                    sb = new StringBuilder();
                }
            }
            if (chain.isEmpty()) {
                LOG.error("Certificate chain is empty");
                return null;
            }
        } catch (Exception e) {
            LOG.error("Error occurred when getting certificate chain for connector", e);
            return null;
        }
        return chain;
    }

    public static char[] getDefaultKeyStorePassword() {
        return KEY_STORE_PASSWORD.toCharArray();
    }
}
