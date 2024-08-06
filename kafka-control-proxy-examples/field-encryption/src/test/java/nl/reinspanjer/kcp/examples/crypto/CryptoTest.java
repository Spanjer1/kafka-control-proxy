package nl.reinspanjer.kcp.examples.crypto;

import nl.reinspanjer.kcp.examples.crypto.impl.SimpleLocalCrypto;
import org.junit.jupiter.api.Test;

import java.security.GeneralSecurityException;

import static org.assertj.core.api.Assertions.assertThat;


public class CryptoTest {

    @Test
    public void testCrypto() throws GeneralSecurityException {
        CryptoProvider cryptoProvider = new SimpleLocalCrypto();
        cryptoProvider.init();
        String encrypted = cryptoProvider.encrypt("Hello, World!");
        assertThat(encrypted).isNotEqualTo("Hello, World!");
        String decrypted = cryptoProvider.decrypt(encrypted);
        assertThat(decrypted).isEqualTo("Hello, World!");
    }

}
