import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

@SuppressWarnings("unchecked")
public class LinearHash implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static RandomAccess  disk;
	private  static HashMap<String, LinearHash> store;
	
	private HashMap<String,Object> index;
	private String tableName;
	
	 
	
	static {
		try {
			store = (HashMap<String, LinearHash>) Serializer.fileDeserialize(Constant.LH_SERIAL_PATH);
			if (null == store) store = new HashMap<String, LinearHash>();  
			disk = new RandomAccess(Constant.DISK_PATH);
			saveState();
			
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
 	private LinearHash(String tableName) throws ClassNotFoundException, IOException {
		this.tableName = tableName;
		
		try {
			
			if (store.containsKey(tableName)) {
				index = store.get(tableName).index;
			} else {
				store.put(tableName, this);
			}
			
			
		} catch (Exception e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		if (null == index) {
			System.out.println(" deserial didn't happen");
			index = new HashMap<String, Object>();
			index.put("M", Constant.M_INIT);
			index.put("SP",Constant.SP_INIT);

			ArrayList<Integer> chains = new ArrayList<Integer>();
			for(int i =0; i < 2 * (int) Constant.M_INIT; i++){
				chains.add(-1);
			}
			index.put("chains", chains);
		}
	}
			
	 int  getM() {
		
		return (int) index.get("M");
	}
	
	 int getSP() {
		return (int) index.get("SP");
	}
	 
	 List<Integer> getChains() {
			return (List<Integer>) index.get("chains");
		} 
	
	 void setM(int M) throws IOException {
		 index.put("M", M);
		 saveState();
	 }
	 
	 void setSP(int sp) throws IOException {
		 index.put("SP", sp);
		 saveState();
	 }
	 void setChains(ArrayList<Integer> chains) throws IOException {
		 index.put("chains", chains);
		 saveState();
	 }
	
	 
	public  int Hash(int record_key) {	
		int m = record_key % getM();
		int sp = getSP();
		if (m < sp) {
			m = record_key % (2 * getM());
		}
		return m;
	}

	public  void InsertTuple(byte[] tuple) throws ClassNotFoundException, IOException {
		
		byte[] newTuple = new byte[Tuple.TupleSize(this.getTableName())];
		System.arraycopy(tuple, 0, newTuple, 0, tuple.length);
		tuple = newTuple;
		
		byte [] key = Tuple.readKey(this.getTableName(), tuple);
		int chain_no = Hash(Tuple.hash(key));
		
		List<Integer> chains = (ArrayList<Integer>) this.getChains();
		
		int firstPageID = (int) chains.get(chain_no);

		byte [] pgBuf = new byte[Page.PAGE_SIZE];
		
		if (-1 == firstPageID) {				
			
			// get new page - allocated and read to a buf
			int new_pg_no = getNewPageBuf(pgBuf);
			
					
			/*
			 * we are changing the LH index.chains and
			 * record the state : persist
			 */	
			chains.set(chain_no, new Integer(new_pg_no));
			this.setChains((ArrayList<Integer>) chains);
			
			
			firstPageID = new_pg_no;
		} else {
			
			getDisk().readPage(pgBuf, firstPageID);
			
		}		
		
		Page.ADD_STATUS updated_status = Page.addTuple(this, pgBuf, tuple);
		
		if (Page.ADD_STATUS.SUCCESS != updated_status) { 
			/*
			 *  why wasn't the tuple added? 
			 *  or is it some thing else?
			 *  
			 */
		} 
	}
	
	public byte[] Search(byte[] key) {
		
		byte[] tuple = new byte[Tuple.TupleSize(this.getTableName())];
		int chain_no = Hash(Tuple.hash(key));
		
		//System.out.println(chain_no);
		
		int first_pg_no = getChains().get(chain_no);
		
		if (-1 != first_pg_no) { 
			
			byte[] pgBuf = new byte[Page.PAGE_SIZE];
		
			getDisk().readPage(pgBuf, first_pg_no);
		
			System.out.print("\n-------------------\nSearching...");
		
			tuple = Page.SearchTuple(this.getTableName(), pgBuf, key);
			
		}
		if (null == tuple) {
			System.out.println("**tuple for "+ new String(key) + " doesn't exist**");
		}
		return tuple;
	}
	
	
	public static RandomAccess getDisk() {
		return disk;
	}

	public static void setDisk(RandomAccess dsk) {
		disk = dsk;
	}

		
	
	/*
	 * persist index for future runs
	 */
	public static void saveState() throws IOException {
		/*
		 * record the linear hash to the serialized file : persistent storage!
		 */
		Serializer.fileSerialize(store, Constant.LH_SERIAL_PATH);
			
	}
		
	
	/*
	 * For the purposes of looking at the chain structure:
	 * HELPS:
	 * 		debugging
	 * 		illustration 
	 */
	public  String showLinearHash() {
		List<Integer> chains = this.getChains();
		StringBuffer strBuff = new StringBuffer();
		
		for (int i = 0; i < chains.size(); i++) {
			int first_pg_no = (Integer)  chains.get(i);
			
			strBuff.append(first_pg_no + "->");
			
			if (-1 == first_pg_no) {
				strBuff.append("\n");
				continue;
			}
			
			byte[] firstPage = new byte[Page.PAGE_SIZE];
			getDisk().readPage(firstPage, first_pg_no);
			Page.getLastPage(firstPage, strBuff);
			
			strBuff.append( "#\n");
			
		}
		System.out.println(strBuff.toString());
		return strBuff.toString();
	}
	
	
	/*
	 * ARG: pageBuf - byte buffer of size PAGE_SIZE 
	 * 
	 * given pageBuf:
	 * 		allocates a page on disk
	 * 		initializes headers in pageBuf
	 * 		writes back updated pageBuf to page on disk
	 * 		returns the page number
	 */
	public static int getNewPageBuf(byte[] pageBuf)  {
		
		
		if (Page.PAGE_SIZE > pageBuf.length) {
			System.err.println("Buffer underflow for new page creation, ");
			System.exit(0);
		}
		
		Arrays.fill(pageBuf, (byte) 0);
		
		// allocate a new page on disk
		int new_page_no =-1;
		try {
			new_page_no = LinearHash.getDisk().allocatePage();
		
		//initialize the new page buffer with allocated page number and other header attr.
		Page.initPageBuf(pageBuf, new_page_no);
		
		//write the page buf back to disk
		LinearHash.getDisk().writePage(pageBuf, new_page_no);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new_page_no;
	}
	
	public  Double getAverageChainLength() {
		int no_of_pages = 0;
		List<Integer> chains = (List<Integer>) this.getChains();
 		for (int i=0; i< chains.size();i++) {
			if (-1 < chains.get(i)) {
				byte [] firstPage = new byte[Page.PAGE_SIZE];
				getDisk().readPage(firstPage, chains.get(i));
				no_of_pages += Page.getChainLength(firstPage);
			}
		}
 		
 		int active_chains_no = getNoOfActiveChains();
 		
 		return ( (double)no_of_pages/(double) active_chains_no);
	}

	private  int getNoOfActiveChains() {
		int M = this.getM();
		int sP = this.getSP();
		return M + sP;
	}

	public void importData(String fileName) throws IOException, ClassNotFoundException {
		String csvfile = Constant.BASE_PATH + fileName;
		
		String ext = fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length());
		
		if (!ext.equals("csv")) {
			System.err.println("\nImport data must be in CSV format!\n");
			return;
		}
		
		String line = "";
		String split = ",";
		BufferedReader br = null;
		List<ArrayList<String>> data = null;
		
	try {
		 br = new BufferedReader(new FileReader(csvfile));
		 data = new ArrayList<ArrayList<String>>();
	} catch(FileNotFoundException f) {
		System.err.println("All temp/config files need to be in $CyDIW_HOME/cyclients/linearhash/workspace");
		System.exit(1);
	}	
		while((line = br.readLine()) != null)
		{
			StringTokenizer st = new StringTokenizer(line,split);
            ArrayList<String> row = new ArrayList<String>();
			while (st.hasMoreTokens())
            {
            	row.add(st.nextToken());
            	
            }
			data.add(row);
		}
		br.close();
		
		List<TupleAttribute> tupleConfig = Tuple.tupleAttr.get(this.getTableName());
		int tupleSize = Tuple.TupleSize(this.getTableName());
	
		for(int i = 0; i< data.size(); i++)
		{
			byte [] tuple = new byte[tupleSize];
			List<String> row = data.get(i);
			
			for (int j =0; j < row.size(); j++) {
				TupleAttribute attr = tupleConfig.get(j);
				String colData = Util.truncate(row.get(j), attr.getSize());
				int colIndex = attr.startIndex(tupleConfig);
				
				System.arraycopy(colData.getBytes(), 0, tuple, colIndex, colData.length());
			}
			String keyString = new String(Tuple.readKey(getTableName(), tuple));
			
			this.InsertTuple(tuple);				
		}
			
	
	}
	
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public static void main(String args[] ) throws IOException, ClassNotFoundException {
		
		LinearHash lHash = getLinHash("employees");
		
		//lHash.importData("Employees.csv");
	
		lHash.InsertTuple("TejaswiniGMSCSE".getBytes());
		
		lHash.showLinearHash();
		
		lHash.Search("TejaswiniG".getBytes());
		
		//System.out.println("\n memory status:");
		//getDisk().DiskStatus();
	}

	public static LinearHash getLinHash(String tableName) throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
		if (store.containsKey(tableName)) {
			return store.get(tableName);
		}
		
		if (!Tuple.tupleAttr.containsKey(tableName)) {
			
			System.err.println("Table Definition not found! please add table to tuple_config.xml");
			return null;
		}
		
		return new LinearHash(tableName);
	}

	public static int countPagesInUse() {
		// TODO Auto-generated method stub
		int pages_used = 0;
		
		for (String tableName :store.keySet()) {
			for (int pg_no :store.get(tableName).getChains()) {
				if (-1 == pg_no) continue;
				byte firstPg [] = new byte [Page.PAGE_SIZE];
				getDisk().readPage(firstPg, pg_no);
				int pages_in_chain = Page.getChainLength(firstPg);
				pages_used += pages_in_chain;
			}
		}
		
		return pages_used;
	}
	

}
