package heap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Provides methods to serialize and deserialize objects.
 * @author anurag
 *
 */
public class SerializationUtil {
	/**
	 * Returns the serialized form of object.
	 * 
	 * @param object Object to serialize
	 * @return Serialized form of object. Null returned in case of an exception.
	 */
	public static byte[] getSerializedForm(Object object){
		byte[] serializedData = null;
		
		try{
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			
			objectOutputStream.writeObject(object);
			objectOutputStream.flush();
			byteArrayOutputStream.flush();
			
			serializedData = byteArrayOutputStream.toByteArray();
			
			// Close streams.
			objectOutputStream.close();
			byteArrayOutputStream.close();
		}catch(Exception exception){
			// Do nothing.
			exception.printStackTrace();
		}
		
		return serializedData;
	}
	
	/**
	 * Returns the deserialized form of data.
	 * 
	 * @param data Data to deserialize.
	 * @return Deserialized form of data.
	 */
	public static Object getDeserializedForm(byte[] data){
		Object object = null;
		
		try{
			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
			ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
			
			object = objectInputStream.readObject();
			
			// Close streams.
			objectInputStream.close();
			byteArrayInputStream.close();
		}catch(Exception exception){
			// Do nothing.
		}
		
		return object;
	}
}
