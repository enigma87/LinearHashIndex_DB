import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class LinearHash {
	
	private  static LinearHash linHash ;
	public HashMap<String,Object> index;
	
	public static RandomAccess  disk; 
	
	static {
		try {
			linHash = new LinearHash();
			linHash.saveState();
			
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static LinearHash getLinHash(){
		return linHash;
	}
	
	
 	private LinearHash() throws ClassNotFoundException, IOException {
		disk = new RandomAccess(Constant.DISK_PATH);
		
		try {
			index = (HashMap<String, Object>) Serializer.fileDeserialize(Constant.LH_SERIAL_PATH);
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		catch(FileNotFoundException e) {
			/*
			 * if file not found, carry on as usual, we will generate it anyway
			 */
		}catch (IOException e) {
			
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
		
		return (int) linHash.index.get("M");
	}
	
	 int getSP() {
		return (int) linHash.index.get("SP");
	}
	 
	 List<Integer> getChains() {
			return (List<Integer>) linHash.index.get("chains");
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
		byte [] key = Tuple.readKey(tuple);
		int chain_no = Hash(Tuple.hash(key));
		
		List<Integer> chains = (ArrayList<Integer>) linHash.getChains();
		
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
			linHash.setChains((ArrayList<Integer>) chains);
			
			
			firstPageID = new_pg_no;
		} else {
			
			getDisk().readPage(pgBuf, firstPageID);
			
		}		
		
		Page.ADD_STATUS updated_status = Page.addTuple(pgBuf, tuple);
		
		if (Page.ADD_STATUS.SUCCESS != updated_status) { 
			/*
			 *  why wasn't the tuple added? 
			 *  or is it some thing else?
			 *  
			 */
		} 
	}
	
	public static RandomAccess getDisk() {
		return disk;
	}

	public static void setDisk(RandomAccess dsk) {
		disk = dsk;
	}

	public static void main(String args[] ) throws IOException, ClassNotFoundException {
		
		LinearHash lHash = getLinHash();
		
		String [] dnames = new String []{"finance","acounting", "pr", "crm", "oions", "sales", "marketing","finance","accoung", "prod", "crmod", "opeions", "saasdes", "maasdking"};
		String [] mnames = new String[] {"alberto", "bobo", "cassandra", "Dreyfus", "Eliza", "Fanny", "George","alberto", "bobo", "cassandra", "Dreyfus", "Eliza", "Fanny", "George"};
		
		
		for (int i: Util.range(0, dnames.length, 1)) {	
			byte [] tuple = new byte[Tuple.TUPLE_SIZE];
			System.arraycopy(Util.rightPadChar(dnames[i], Tuple.DNAME_SIZE, Tuple.NAME_PAD).getBytes("UTF8"), 0, tuple, 0, Tuple.DNAME_SIZE);
			System.arraycopy(Util.rightPadChar(mnames[i], Tuple.MNAME_SIZE, Tuple.NAME_PAD).getBytes("UTF8"), 0, tuple, Tuple.DNAME_SIZE, Tuple.MNAME_SIZE);
			
		//	lHash.setSP(1);
			
		//	System.out.print(Tuple.hash(Tuple.readKey(tuple)) + " " + lHash.Hash(Tuple.hash(Tuple.readKey(tuple))) + "\n");
			
			lHash.InsertTuple(tuple);

		}
		
		
		showLinearHash();
	}
	
	
	
	/*
	 * persist index for future runs
	 */
	public void saveState() throws IOException {
		/*
		 * record the linear hash to the serialized file : persistent storage!
		 */
		Serializer.fileSerialize(linHash.index, Constant.LH_SERIAL_PATH);
			
	}
		
	
	/*
	 * For the purposes of looking at the chain structure:
	 * HELPS:
	 * 		debugging
	 * 		illustration 
	 */
	public static void showLinearHash() {
		List chains = linHash.getChains();
		
		for (int i = 0; i < chains.size(); i++) {
			int first_pg_no = (Integer)  chains.get(i);
			System.out.print(first_pg_no + "->");
			if (-1 == first_pg_no) {
				System.out.println();
				continue;
			}
			
			byte[] firstPage = new byte[Page.PAGE_SIZE];
			linHash.disk.readPage(firstPage, first_pg_no);
			Page.getLastPage(firstPage, true);
			System.out.print("#");
			System.out.println("");
		}
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
	public static int getNewPageBuf(byte[] pageBuf) throws IOException {
		
		if (Page.PAGE_SIZE > pageBuf.length) {
			System.err.println("Buffer underflow for new page creation, ");
			System.exit(0);
		}
		// allocate a new page on disk
		int new_page_no = LinearHash.getDisk().allocatePage();
		
		//initialize the new page buffer with allocated page number and other header attr.
		Page.initPageBuf(pageBuf, new_page_no);
		
		//write the page buf back to disk
		LinearHash.getDisk().writePage(pageBuf, new_page_no);
		
		return new_page_no;
	}
	
	public static Double getAverageChainLength() {
		int no_of_pages = 0;
		List<Integer> chains = (List<Integer>) linHash.getChains();
 		for (int i=0; i< chains.size();i++) {
			if (-1 < chains.get(i)) {
				byte [] firstPage = new byte[Page.PAGE_SIZE];
				linHash.disk.readPage(firstPage, chains.get(i));
				no_of_pages += Page.getChainLength(firstPage);
			}
		}
 		
 		int active_chains_no = getNoOfActiveChains();
 		
 		return ( (double)no_of_pages/(double) active_chains_no);
	}

	private static int getNoOfActiveChains() {
		int M = linHash.getM();
		int sP = linHash.getSP();
		return M + sP;
	}
	
}
