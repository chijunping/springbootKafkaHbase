package com.zhibo8.warehouse.commons;


import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.security.Security;
import java.util.Map;

public class SecurityUtils {
    private static final Logger logger = LoggerFactory.getLogger(SecurityUtils.class);
    public static final String key = "!C9!NfX8njSO$OK!";

    public static String decrypt(String input) {
        if (StringUtils.isBlank(input) || "null".equals(input)) return null;
        try {
            //通过BouncyCastle组件来让java里面支持PKCS7Padding填充。在加解密之前加上：Security.addProvider(new BouncyCastleProvider())，
            Security.addProvider(new BouncyCastleProvider());//
            SecretKeySpec skey = new SecretKeySpec(key.getBytes(), "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS7Padding");
            cipher.init(Cipher.DECRYPT_MODE, skey);
            byte[] output = cipher.doFinal(Base64.decode(input.getBytes("utf-8"), Base64.DEFAULT));
            return new String(output);
        } catch (Exception e) {
            logger.error(e.getMessage()+"=>inputString="+input, e);
        }
        return null;
    }

    public static String encrypt(String input) {
        try {
            //通过BouncyCastle组件来让java里面支持PKCS7Padding填充。在加解密之前加上：Security.addProvider(new BouncyCastleProvider())，
            Security.addProvider(new BouncyCastleProvider());//
            SecretKeySpec skey = new SecretKeySpec(key.getBytes(), "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS7Padding");
            cipher.init(Cipher.ENCRYPT_MODE, skey);
            byte[] crypted = cipher.doFinal(input.getBytes());
            return new String(Base64.encode(crypted, Base64.DEFAULT));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public static String SHA1(String decript) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            digest.update(decript.getBytes());
            byte messageDigest[] = digest.digest();
            // Create Hex String
            StringBuffer hexString = new StringBuffer();
            // 字节数组转换为 十六进制 数
            for (int i = 0; i < messageDigest.length; i++) {
                String shaHex = Integer.toHexString(messageDigest[i] & 0xFF);
                if (shaHex.length() < 2) {
                    hexString.append(0);
                }
                hexString.append(shaHex);
            }
            return hexString.toString();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return "";
    }

    @Test
    public void TestEncrypt() {

        String jsonData = " {\n" +
                "\t\t\"event\": \"退出页面 \",\n" +
                "\t\t\"model\": \"新闻内页 \",\n" +
                "\t\t\"params\": {\n" +
                "\t\t\t\"duration\": \"19 \",\n" +
                "\t\t\t\"from\": \"新闻_最新 \",\n" +
                "\t\t\t\"title\": \"追梦妈妈，我和kd关很好.... \",\n" +
                "\t\t\t\"type\": \"basketball\",\n" +
                "\t\t\t\"url\": \"/nba/2018-05-19/dsfsfsfasf.htm \"\n" +
                "\t\t},\n" +
                "\t\t\"type\": \"view \"\n" +
                "\t}\n";

        String encrypt = encrypt(jsonData);
        String decrypt = decrypt(encrypt);
        Map decryptMap = JSON.parseObject(decrypt, Map.class);

        System.out.println(encrypt);
        System.out.println(decryptMap);
    }
}
