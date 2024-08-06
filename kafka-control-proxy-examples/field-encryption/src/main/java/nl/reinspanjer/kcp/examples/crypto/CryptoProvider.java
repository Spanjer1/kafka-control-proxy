package nl.reinspanjer.kcp.examples.crypto;

import java.security.GeneralSecurityException;

public interface CryptoProvider {

    void init() throws GeneralSecurityException;

    String encrypt(String data) throws GeneralSecurityException;

    String decrypt(String encryptedData) throws GeneralSecurityException;

}
