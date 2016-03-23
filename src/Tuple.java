public class Tuple {

	public static final int TUPLE_SIZE	= 20;
	public static final int DNAME_SIZE = 10;
	public static final int MNAME_SIZE = 10;
	public static final char NAME_PAD = '#';
	
	/*
	 * IMPORTANT : we only write to and read (key & name) from a copy of the tuple from buffer.
	 * we should again write the tuple back to buffer ( and then back to writePage)
	 */
	
	public static byte[] readKey(byte[] tuple) {
		byte key [] = new byte[DNAME_SIZE];
		System.arraycopy(tuple, 0, key, 0, DNAME_SIZE);
		return key;
	}
	
	public static void writeKey(byte [] tuple, byte[] dname) {
		System.arraycopy(dname, 0, tuple, 0, DNAME_SIZE);
	}
	
	public  static byte[] readMName(byte[] tuple) {
		byte dname [] = new byte[MNAME_SIZE];
		System.arraycopy(tuple, DNAME_SIZE, dname, 0, MNAME_SIZE);
		return dname;
	}
	
	public static void writeName(byte [] tuple, byte[] mname) {
		System.arraycopy(mname, 0, tuple, DNAME_SIZE, MNAME_SIZE);
	}
	
	public static byte[] giveTuple(byte[] dname, byte[] mname) {
		byte[] tuple = new byte[TUPLE_SIZE];
		System.arraycopy(dname, 0, tuple, 0, DNAME_SIZE);
		System.arraycopy(mname, 0, tuple, DNAME_SIZE, MNAME_SIZE);
		return tuple;
	}
	
	public static int hash(byte[] inp) {
		
		final int PRIME = 907;
		
		int asciisum = 0;
		
		for (byte b : inp) {
			asciisum += Byte.toUnsignedInt(b);
		}
		//System.out.println("\t\t\t" + asciisum% PRIME);
		return asciisum%PRIME;
	}

	
	
}
