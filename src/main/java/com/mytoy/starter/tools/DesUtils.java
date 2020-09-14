package com.mytoy.starter.tools;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;

/**
 * @author
 * @date 2019/07/23
 */
public class DesUtils {

    private final static Logger logger = LoggerFactory.getLogger(DesUtils.class);

    /**
     * 加密
     *
     * @param str      byte[]
     * @param password String
     * @return byte[]
     */
    public static String encrypt(String password, String str) {

        try {
            byte[] datasource = str.getBytes("UTF-8");
            SecureRandom random = new SecureRandom();
            DESKeySpec desKey = new DESKeySpec(password.getBytes());
            //创建一个密匙工厂，然后用它把DESKeySpec转换成
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            SecretKey securekey = keyFactory.generateSecret(desKey);
            //Cipher对象实际完成加密操作
            Cipher cipher = Cipher.getInstance("DES");
            //用密匙初始化Cipher对象,ENCRYPT_MODE用于将 Cipher 初始化为加密模式的常量
            cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
            //现在，获取数据并加密 正式执行加密操作
            //按单部分操作加密或解密数据，或者结束一个多部分操作
            byte[] bytes = cipher.doFinal(datasource);
            String result = Base64.encodeBase64String(bytes);
            return result;
        } catch (Throwable e) {
            logger.error("DES 加密异常，详情：" + e.toString());
            e.printStackTrace();
        }
        return null;
    }

    public static String decrypt(String password, String encryptResult) throws Exception {
//        byte[] strbytes = Base64.decodeBase64(encryptResult);
        byte[] strbytes = encryptResult.getBytes();
//        byte[] src = datas.getBytes();
        // DES算法要求有一个可信任的随机数源
        SecureRandom random = new SecureRandom();
        // 创建一个DESKeySpec对象
        DESKeySpec desKey = new DESKeySpec(password.getBytes());
        // 创建一个密匙工厂
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");//返回实现指定转换的 Cipher 对象
        // 将DESKeySpec对象转换成SecretKey对象
        SecretKey securekey = keyFactory.generateSecret(desKey);
        // Cipher对象实际完成解密操作
        Cipher cipher = Cipher.getInstance("DES");
        // 用密匙初始化Cipher对象
        cipher.init(Cipher.DECRYPT_MODE, securekey, random);
        // 真正开始解密操作
        byte[] bytes = cipher.doFinal(strbytes);
        String result = new String(bytes);
//        String result = Base64.encodeBase64String(bytes);
        return result;
    }
}
