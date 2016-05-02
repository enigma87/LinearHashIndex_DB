import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Serializer {

    public static byte[] byteSerialize(Object obj) throws IOException {
        try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
            try(ObjectOutputStream o = new ObjectOutputStream(b)){
                o.writeObject(obj);
            }
            return b.toByteArray();
        }
    }

    public static Object byteDeserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
            try(ObjectInputStream o = new ObjectInputStream(b)){
                return o.readObject();
            }
        }
    }

    public static void fileSerialize(Object obj, String path) throws IOException{
    	File serFile = new File(path);
    	if (!serFile.exists()) {
    		serFile.createNewFile();
    	}
    	FileOutputStream fout = new FileOutputStream(serFile);
    	ObjectOutputStream oos = new ObjectOutputStream(fout);
    	oos.writeObject(obj);
    	oos.close();
    }
    
    public static Object fileDeserialize(String path) throws IOException, ClassNotFoundException {
    	File serFile = new File(path);
    	if (!serFile.exists()) return null;
    	FileInputStream fi = new FileInputStream(serFile);
    	ObjectInputStream oi = new ObjectInputStream(fi);
    	Object obj = oi.readObject();
    	oi.close();
    	return obj;	
    }
}