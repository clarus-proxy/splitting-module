package eu.clarussecure.dataoperations.kriging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;


public class Crypto {
	
	  public static final String PRIVATE_KEY = Constants.carpetaKeys+"private.key";
	  public static final String PUBLIC_KEY = Constants.carpetaKeys+"public.key";
	  
	  public static byte[] encrypt(String text, String key) throws FileNotFoundException, IOException, ClassNotFoundException {
		  return encrypt(text.getBytes(), key);
	  }
	  public static byte[] encrypt(byte[] text, String key) throws FileNotFoundException, IOException, ClassNotFoundException {
		  ObjectInputStream keyStream = new ObjectInputStream(new FileInputStream(key));
		  final PublicKey publicKey = (PublicKey) keyStream.readObject(); //la key no ha de canviar per a res, aixi que la definim com a final
		  keyStream.close();
		  return encrypt(text, publicKey);
	  }
	  public static byte[] encrypt(byte[] text, PublicKey key) {
	    byte[] cipherText = null;
	    
	    try {
	      final Cipher cipher = Cipher.getInstance(Constants.algoritmeXifrat);
	      cipher.init(Cipher.ENCRYPT_MODE, key);
	      cipherText = cipher.doFinal(text);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	    return cipherText;
	  }

	  
	  public static byte[] decrypt(String text, String key) throws FileNotFoundException, IOException, ClassNotFoundException {
		  return decrypt(text.getBytes(), key);
	  }
	  public static byte[] decrypt(byte[] text, String key) throws FileNotFoundException, IOException, ClassNotFoundException {
		  ObjectInputStream keyStream = new ObjectInputStream(new FileInputStream(key));
		  final PrivateKey publicKey = (PrivateKey) keyStream.readObject();
		  keyStream.close();
		  return decrypt(text, publicKey);
	  }
	  public static byte[] decrypt(byte[] text, PrivateKey key) {
	    byte[] dectyptedText = null;
	    try {
	      final Cipher cipher = Cipher.getInstance(Constants.algoritmeXifrat);
	      cipher.init(Cipher.DECRYPT_MODE, key);
	      dectyptedText = cipher.doFinal(text);
	    } catch (Exception ex) {
	      ex.printStackTrace();
	    }

	    return dectyptedText;
	  }
	  
	  public static void generateKey() {
		    try {
		      final KeyPairGenerator keyGen = KeyPairGenerator.getInstance(Constants.algoritmeXifrat);
		      keyGen.initialize(512);
		      final KeyPair key = keyGen.generateKeyPair();

		      File privateKeyFile = new File(PRIVATE_KEY);
		      File publicKeyFile = new File(PUBLIC_KEY);

		      // Create files to store public and private key
		      if (privateKeyFile.getParentFile() != null) {
		        privateKeyFile.getParentFile().mkdirs();
		      }
		      privateKeyFile.createNewFile();

		      if (publicKeyFile.getParentFile() != null) {
		        publicKeyFile.getParentFile().mkdirs();
		      }
		      publicKeyFile.createNewFile();

		      // Saving the Public key in a file
		      ObjectOutputStream publicKeyOS = new ObjectOutputStream(
		          new FileOutputStream(publicKeyFile));
		      publicKeyOS.writeObject(key.getPublic());
		      publicKeyOS.close();

		      // Saving the Private key in a file
		      ObjectOutputStream privateKeyOS = new ObjectOutputStream(
		          new FileOutputStream(privateKeyFile));
		      privateKeyOS.writeObject(key.getPrivate());
		      privateKeyOS.close();
		    } catch (Exception e) {
		      e.printStackTrace();
		    }

		  }
}
