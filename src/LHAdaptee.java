import java.io.IOException;

public class LHAdaptee {

	public static void LHImport(String tableName, String fileName) {
		try {
			LinearHash lhash = LinearHash.getLinHash(tableName);
			if (null != lhash) lhash.importData(fileName);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static boolean InsertTuple(String tableName, byte[] tuple) {
		boolean insert = true;
		try {
			LinearHash lhash = LinearHash.getLinHash(tableName);
		
			if (null != lhash) {
				lhash.InsertTuple(tuple);
			} else {
				insert = false;
			}
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return insert;
	}
	
	public static byte[] Search(String tableName, byte[] keyValue){
		LinearHash lhash = null;
		try {
			lhash = LinearHash.getLinHash(tableName);
			if (null != lhash) lhash.Search(keyValue);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lhash.Search(keyValue);
		
	}
	
	public static String MemoryStatus() {
		String diskstatus = LinearHash.getDisk().DiskStatus();
		Integer pagesUsed = LinearHash.countPagesInUse();
		return "\ndisk status:" + diskstatus + "<br>pages in use:" + pagesUsed.toString() ;
	}
	
	public static String ShowLinHash(String tableName) {
		try {
			LinearHash lhash = LinearHash.getLinHash(tableName);
			String lhash_string = null;
			if (null != lhash) {
				Double avg_length = lhash.getAverageChainLength();
				lhash_string = "<b>Avg chain length: "+ avg_length.toString() +"</b><br>";
				lhash_string = lhash.showLinearHash();
				
			}
			return lhash_string;
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "not a table!";
	}

}
