package nl.reinspanjer.kcp.examples.crypto.impl;

import nl.reinspanjer.kcp.examples.crypto.CryptoProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.GeneralSecurityException;
import java.util.Base64;

public class SimpleLocalCrypto implements CryptoProvider {
    private static final String ALGO = "AES";
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLocalCrypto.class);
    private static Cipher cipher;
    private static SecretKey key;

    public void init() throws GeneralSecurityException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(ALGO);
        cipher = Cipher.getInstance(ALGO);
        keyGenerator.init(256);
        key = keyGenerator.generateKey();
        cipher.init(Cipher.ENCRYPT_MODE, key);
    }

    public String encrypt(String data) throws GeneralSecurityException {

        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] encryptedBytes = cipher.doFinal(data.getBytes());
        return Base64.getEncoder().encodeToString(encryptedBytes);

    }

    public String decrypt(String encryptedData) throws GeneralSecurityException {
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] decodedBytes = Base64.getDecoder().decode(encryptedData);
        byte[] decryptedBytes = cipher.doFinal(decodedBytes);
        return new String(decryptedBytes);
    }


}


