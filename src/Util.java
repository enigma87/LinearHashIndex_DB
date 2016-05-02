import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Util {

	public Util() {
		// TODO Auto-generated constructor stub
	}

	
	
	public static String rightPadChar(String str, int num, char c) {
	    return String.format("%1$-" + num + "s", str).replace(' ', c);
	}
	
	public static byte[] StringToBytes(String s) throws UnsupportedEncodingException {
		return s.getBytes("UTF8");
	} 
	
	public static int[] range(int start, int end, int incr) {
		ArrayList<Integer> range = new ArrayList<Integer>();
		for (int i = start; i < end; i += incr) {
			range.add(i);
		}
		int a [] = new int[range.size()];
		
		for (int i = 0; i < a.length; i++ ) {
			a[i] = range.get(i);
		}
		
		return a;
	}

	public static int byteToInt(byte[] byteInt) throws ClassNotFoundException, IOException {
		return ByteBuffer.wrap(byteInt).getInt();
	}
	
	public static byte [] intToByte(int i) {
		return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
	}  
	
	 public static String truncate(String value, int length) {
			// Ensure String length is longer than requested size.
		if (value.length() > length) {
			return value.substring(0, length);
		} else {
			return value;
		}
	 }
	
}
