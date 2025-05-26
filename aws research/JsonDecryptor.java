
import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.security.spec.*;
import java.util.Base64;
import javax.crypto.*;
import javax.crypto.spec.*;

import org.json.JSONObject;
import java.util.zip.GZIPInputStream;

public class JsonDecryptor {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java JsonDecryptor <jsonFile> <privateKeyPem>");
            return;
        }

        String jsonPath = args[0];
        String privKeyPath = args[1];

        JSONObject payload = new JSONObject(new String(Files.readAllBytes(Paths.get(jsonPath))));

        byte[] encryptedKey = Base64.getDecoder().decode(payload.getString("enc.key"));
        byte[] iv = Base64.getDecoder().decode(payload.getString("iv"));
        byte[] encryptedData = Base64.getDecoder().decode(payload.getString("payload"));

        PrivateKey privateKey = loadPrivateKey(privKeyPath);
        byte[] aesKey = decryptRSA(encryptedKey, privateKey);
        byte[] decryptedData = decryptAES(encryptedData, aesKey, iv);
        byte[] uncompressed = decompressGzip(decryptedData);

        String outputFile = jsonPath.replace(".encrypted.json", ".restored.txt");
        Files.write(Paths.get(outputFile), uncompressed);
        System.out.println("[+] File restored: " + outputFile);
    }

    public static PrivateKey loadPrivateKey(String filename) throws Exception {
        String pem = new String(Files.readAllBytes(Paths.get(filename)))
                         .replaceAll("-----\w+ PRIVATE KEY-----", "").replaceAll("\s", "");
        byte[] der = Base64.getDecoder().decode(pem);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
        KeyFactory factory = KeyFactory.getInstance("RSA");
        return factory.generatePrivate(spec);
    }

    public static byte[] decryptRSA(byte[] data, PrivateKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        cipher.init(Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(data);
    }

    public static byte[] decryptAES(byte[] encrypted, byte[] key, byte[] iv) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        IvParameterSpec ivSpec = new IvParameterSpec(iv);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
        return cipher.doFinal(encrypted);
    }

    public static byte[] decompressGzip(byte[] compressed) throws IOException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(compressed);
        GZIPInputStream gzip = new GZIPInputStream(byteStream);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        byte[] buffer = new byte[4096];
        int len;
        while ((len = gzip.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
        return out.toByteArray();
    }
}
