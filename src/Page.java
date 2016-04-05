import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Page {
	
	public final static int  PAGE_SIZE = 200;
	public final static int HEADER_LEN = 14;
	
	public final static int MAX_TUPLES =  (2* (Page.PAGE_SIZE/Tuple.TupleSize()))/3;
	
	public static final HashMap<PAGE_ITEMS, int[]> pageOffset;
	
	public enum PAGE_ITEMS {PAGE_LEN, HEADER_LEN, PAGE_NO, NEXTPAGE_NO, NO_OF_TUPLES, TUPLE_START}
	
	public enum ADD_STATUS {SUCCESS, PAGE_FULL, PAGE_SPLIT}
	
	public enum ITER_STATUS {NEXT_PAGE};

	
	static {
		pageOffset = new HashMap<PAGE_ITEMS, int[]>();
		pageOffset.put(PAGE_ITEMS.PAGE_LEN, new int[] {0, 2});
		pageOffset.put(PAGE_ITEMS.HEADER_LEN, new int[] {2, 2});
		pageOffset.put(PAGE_ITEMS.PAGE_NO, new int[] {4, 4});
		pageOffset.put(PAGE_ITEMS.NEXTPAGE_NO, new int[]{8, 4});
		pageOffset.put(PAGE_ITEMS.NO_OF_TUPLES, new int[] {12, 2});
		pageOffset.put(PAGE_ITEMS.TUPLE_START, new int[] {14,0});
	}
	
	public Page() {
		// TODO Auto-generated constructor stub
	}
	
	
	public static void initPageBuf(byte[] pageBuf, int pg_no) {
		Page.setPageLen(pageBuf, Page.PAGE_SIZE);
		Page.setHeaderLen(pageBuf, Page.HEADER_LEN);
		Page.setPageNo(pageBuf, pg_no);
		Page.setNextPage(pageBuf, -1);
		Page.setNoOfTuples(pageBuf, 0);
	}
	
	
	/*
	 * given a buffer (from readPage?) use the constant page offsets above to 
	 * identify the "pagelength" bytes and return the byte array
	 */	
	public static int getPageLen(byte[] buff) {
		int [] page_len_offset = pageOffset.get(PAGE_ITEMS.PAGE_LEN);
		byte[] pagelen_byte = new byte[page_len_offset[1]]; 
		System.arraycopy(buff, page_len_offset[0], pagelen_byte, 0, page_len_offset[1]);
		ByteBuffer bbuf = ByteBuffer.wrap(pagelen_byte);
		return (int) bbuf.getShort();
	}

	/*
	 * given a buffer (from readPage?) use the constant page offsets above to 
	 * identify the "headerlength" bytes and return the byte array
	 */
	public static int getHeaderLen(byte[] buff) {
		int [] header_len_offset = pageOffset.get(PAGE_ITEMS.HEADER_LEN);
		byte[] headerlen_byte = new byte[header_len_offset[1]]; 
		System.arraycopy(buff, header_len_offset[0], headerlen_byte, 0, header_len_offset[1]);
		ByteBuffer bbuf = ByteBuffer.wrap(headerlen_byte);
		return (int) bbuf.getShort();
	}

	/*
	 * given a buffer (from readPage?) use the constant page offsets above to 
	 * identify the "page_no" bytes and return the byte array
	 */
	public static int getPageNo(byte[] buff) {
		int [] page_no_offset = pageOffset.get(PAGE_ITEMS.PAGE_NO);
		byte[] pageno_byte = new byte[page_no_offset[1]]; 
		System.arraycopy(buff, page_no_offset[0], pageno_byte, 0, page_no_offset[1]);
		ByteBuffer bbuf = ByteBuffer.wrap(pageno_byte);
		return  bbuf.getInt();
	}
	
	/*
	 * given a buffer (from readPage?) use the constant page offsets above to 
	 * identify the "nextpage_no" bytes and return the byte array
	 */
	public static int getNextPage(byte[] buff) {
		int [] next_page_offset = pageOffset.get(PAGE_ITEMS.NEXTPAGE_NO);
		byte[] nextpage_byte = new byte[next_page_offset[1]]; 
		System.arraycopy(buff, next_page_offset[0], nextpage_byte, 0, next_page_offset[1]);
		ByteBuffer bbuf = ByteBuffer.wrap(nextpage_byte);
		return (int) bbuf.getInt();
	}
	
	
	/*
	 * given a buffer (from readPage?) use the constant page offsets above to 
	 * identify the "no_of_tuples" bytes and return the byte array
	 */
	public static int getNoOfTuples(byte[] buff) {
		int [] no_tuples_offset = pageOffset.get(PAGE_ITEMS.NO_OF_TUPLES);
		byte[] notuples_byte = new byte[no_tuples_offset[1]]; 
		System.arraycopy(buff, no_tuples_offset[0], notuples_byte, 0, no_tuples_offset[1]);
		ByteBuffer bbuf = ByteBuffer.wrap(notuples_byte);
		return (int) bbuf.getShort();
	}

	/*
	 * given a buffer (from readPage?) use the constant page offsets above to 
	 * identify the "tuple_index" bytes and return the byte array
	 */
	public static byte[] getTuple(byte[] buff, int indx) {
		byte [] tuple = new byte [Tuple.TupleSize()];
		System.arraycopy(buff, indx  , tuple, 0, Tuple.TupleSize());
		return tuple;
	}
	
	
	/*
	 *  given a page (buffer?) and a data item, use the constant page indices above 
	 *  to locate the position the data bytes go into and then copy the data bytes
	 *  to the appropriate location
	 */
	public static void setPageLen(byte[] buff, int page_len) {
		int [] page_len_offset = pageOffset.get(PAGE_ITEMS.PAGE_LEN);
		short short_pg_len = (short) page_len;
		byte [] data = ByteBuffer.allocate(Short.BYTES).putShort(short_pg_len).array();		
		System.arraycopy(data, 0, buff, page_len_offset[0], data.length);
		return;
	}
	
	public static void setHeaderLen(byte[] buff, int header_len) {
		int [] header_len_offset = pageOffset.get(PAGE_ITEMS.HEADER_LEN);
		short short_header_len = (short) header_len;
		byte [] data = ByteBuffer.allocate(Short.BYTES).putShort(short_header_len).array();		
		System.arraycopy(data, 0, buff, header_len_offset[0], data.length);
		return;
	}
	
	
	public static void setPageNo(byte[] buff, int page_no) {
		int [] page_no_offset = pageOffset.get(PAGE_ITEMS.PAGE_NO);
		int int_pg_no =  page_no;
		byte [] data = ByteBuffer.allocate(Integer.BYTES).putInt(int_pg_no).array();		
		System.arraycopy(data, 0, buff, page_no_offset[0], data.length);
		return;
	}
	
	public static void setNextPage(byte[] buff, int next_page) {
		int [] next_page_offset = pageOffset.get(PAGE_ITEMS.NEXTPAGE_NO);
		int int_next_page =  next_page;
		byte [] data = ByteBuffer.allocate(Integer.BYTES).putInt(int_next_page).array();		
		System.arraycopy(data, 0, buff, next_page_offset[0], data.length);
		return;
	}
	
	public static void setNoOfTuples(byte[] buff, int no_tuples) {
		int [] no_tuples_offset = pageOffset.get(PAGE_ITEMS.NO_OF_TUPLES);
		short short_no_tuples = (short) no_tuples;
		byte [] data = ByteBuffer.allocate(Short.BYTES).putShort(short_no_tuples).array();		
		System.arraycopy(data, 0, buff, no_tuples_offset[0], data.length);
		return;
	}

	public static void setTuple(byte[] buff, int indx, byte[] tuple) {
		System.arraycopy(tuple, 0, buff, indx, Tuple.TupleSize());
	}

	
	/*
	 * 
	 * Note that this method expects first page not to be null;
	 */
	public static int getChainLength(byte[] firstPage) {
		if (null == firstPage) return 0;
		
		int height = 1;
		int next_pg_no = Page.getNextPage(firstPage);
		if (-1 == next_pg_no) return height;
		
		byte [] nextPage = new byte[Page.PAGE_SIZE];
		while(-1 != next_pg_no) {
			height += 1;
			LinearHash.getDisk().readPage(nextPage, next_pg_no);
			next_pg_no = Page.getNextPage(nextPage);
		}	
		return height;
	}
	
	
	public static byte[] getLastPage(byte [] firstPage, boolean print_path) {
			
		int next_pg_no = Page.getNextPage(firstPage);
		if (-1 == next_pg_no) return firstPage;
		
		byte [] lastPage = new byte[Page.PAGE_SIZE];	
		while(-1 != next_pg_no) {
			if (print_path) System.out.print(next_pg_no + "->");
			LinearHash.getDisk().readPage(lastPage, next_pg_no);
			next_pg_no = Page.getNextPage(lastPage);
		}
		
		return lastPage;
	}
	
	public static ADD_STATUS addTuple(byte[] pageBuf, byte [] tuple) throws IOException {
		
		/*
		 *  Get last page in chain
		 */
		byte [] lastPageBuf = getLastPage(pageBuf, false);
		
		ADD_STATUS added = _addTuple(lastPageBuf, tuple);
		
		if (ADD_STATUS.SUCCESS == added) {
			LinearHash.getDisk().writePage(lastPageBuf, getPageNo(lastPageBuf));
			return added;
		} 
		
		else if (ADD_STATUS.PAGE_FULL == added) {
			
			System.err.println(" page full!!, creating new page");
			/*
			 * couldn't add tuple because page reached limit
			 * How to handle! hmm, tricky!
			 * let's say:
			 * 		create a new page-buf
			 * 		write tuple to buf
			 * 		write buf to page(disk) - you will get page_no
			 * 		update page_no to  "lasPageBuf" 		
			 * 		return pageBuf;
			 */
			byte [] newPage = new byte[Page.PAGE_SIZE];
			int new_pg_no = LinearHash.getNewPageBuf(newPage);
			added = _addTuple(newPage, tuple);
			LinearHash.getDisk().writePage(newPage, new_pg_no);
			
			Page.setNextPage(lastPageBuf, new_pg_no);
			LinearHash.getDisk().writePage(lastPageBuf, getPageNo(lastPageBuf));
			
			/*
			 * check for average chain length:
			 * 	split if exceeds lambda-avg
			 */
			if (Constant.LAMBDA_AVG < LinearHash.getAverageChainLength()) {
				System.out.println("Splitting chain " + LinearHash.getLinHash().getSP());
				SplitChain();
				
			}
			
			return added;
		}
	
		return null;
	}
	

	private static void SplitChain() throws IOException {
		/*
		 * use M, sP values to calculate which chain to split
		 * use 3 buffers:
		 * 		one to walk through the chain to split
		 * 		one to write to chain sP
		 * 		one to write to chain M + sP
		 * 
		 * algo:
		 * 		get the first page from index.page[chain_no] to splitBuf
		 * 		set index.page[chain_no] to -1
		 * 		read splitBuf, fill buf1 and buf2
		 * 		when buf1 is full, walk down the chain sP and add page
		 * 		when buf2 is full, walk down the chain M + sP and add page
		 * 		after chain is split : increment sP, and save state
		 * 				if sP > M then change: M  *= 2, sP = 0
		 */
		LinearHash lh = LinearHash.getLinHash();
		int sP = lh.getSP();
		int M = lh.getM();
		
		byte [] splitBuf = new byte[Page.PAGE_SIZE];
		byte [] buf1 = new byte[Page.PAGE_SIZE];
		LinearHash.getNewPageBuf(buf1);
		byte [] buf2 = new byte[Page.PAGE_SIZE];
		LinearHash.getNewPageBuf(buf2);
		
		
		ArrayList<Integer> chains = (ArrayList<Integer>) lh.getChains();
		int split_chain_no = sP;
		
		/*
		 * get the first page and de-link from the chain
		 */
		int first_pg_no = chains.get(sP);
		if  (-1 != first_pg_no) {
			LinearHash.getDisk().readPage(splitBuf, first_pg_no);
		} else {
			Page.initPageBuf(splitBuf, -1);
		}
		chains.set(sP, -1);
		
		lh.setChains(chains); // persist the changes
		
		/*
		  * increment the sP value
		  */
		lh.setSP(sP + 1);
		
		int cur_page_no = getPageNo(splitBuf);
		while(-1 != cur_page_no) {
			/*
			 * read the page into split buf and delete from disk
			 */
			LinearHash.getDisk().readPage(splitBuf, cur_page_no);
			LinearHash.getDisk().deallocatePage(cur_page_no);
			
			/*
			 * get a page tuple iterator and iterate over the page
			 */
			TupleIterator iter = getTupleIterator(splitBuf);
			byte tuple []  = new byte[Tuple.TupleSize()];
			tuple = iter.getNextInPage();
			
			while(null != tuple) {
				/*
				 * we are reading tuples now, split them and put into 
				 * buf1, buf2
				 */
				int new_chain_no = lh.Hash(Tuple.hash(Tuple.readKey(tuple)));
				ADD_STATUS buf_full;
				if (new_chain_no == split_chain_no) {
				
					buf_full = _addTuple(buf1, tuple);
				
					if (ADD_STATUS.PAGE_FULL == buf_full) {
						System.out.println("New page split, buff1 full : add to : " + new_chain_no) ;
						/*
						 * save to disk, the buffer data 
						 */	
						addBuffToChain(buf1, new_chain_no);
					}
				} else  {
				
					
					buf_full = _addTuple(buf2, tuple);
					
					if (ADD_STATUS.PAGE_FULL == buf_full) {
						System.out.println("New page split, buff2 full : add to : " + new_chain_no);
						/*
						 * save to disk, the buffer data 
						 */
						addBuffToChain(buf2, new_chain_no);
					}
				}
			
				tuple = iter.getNextInPage();
			}
			
			cur_page_no = getNextPage(splitBuf);
		
		}
		
			addBuffToChain(buf1, split_chain_no);
			addBuffToChain(buf2, lh.getM() + split_chain_no);
		

			/*
		increase the size of linear hash, if SP > M -1
			 */
		if (lh.getSP() >= lh.getM() ) {
			
			lh.setM(M * Constant.M_INC);
			lh.setSP(Constant.SP_INIT);
			
			for (int i = lh.getM(); i < lh.getM() * Constant.M_INC; i ++) {
				chains.add(i, -1);
				lh.setChains(chains); //persist every change
			}
		}
	}

 /*
 * 	add buff to chain
 */
private static void addBuffToChain(byte [] buf, int new_chain_no) throws IOException {
	
	int pg_no = getPageNo(buf);
	LinearHash.getLinHash().getDisk().writePage(buf, pg_no); 
	
	AddPageToChain(new_chain_no, buf);
	
}


/*
 * given a chain number and a byte buffer, 
 * copy the page number on the byte buffer to the chain's 
 * last page's next_page pointer
 * 
 */
	private static void AddPageToChain(int new_chain_no, byte[] newPgBuf) throws IOException {
		// get page number of buff
		int new_pg_no = getPageNo(newPgBuf);
		
		if (LinearHash.getLinHash().getChains().get(new_chain_no ) == new_pg_no) return;
		
		//get first page of chain
		int first_pg_no = LinearHash.getLinHash().getChains().get(new_chain_no);
		byte [] first_page_buf = new byte[Page.PAGE_SIZE];
		
		
		if (-1 == first_pg_no) {
			ArrayList<Integer> chains = (ArrayList<Integer>) LinearHash.getLinHash().getChains();
			chains.set(new_chain_no, new_pg_no);
			LinearHash.getLinHash().setChains(chains);
			
		} else {
			LinearHash.getDisk().readPage(first_page_buf, first_pg_no);
			byte [] lastPageBuf = getLastPage(first_page_buf, false);
			setNextPage(lastPageBuf, new_pg_no);
			LinearHash.getDisk().writePage(lastPageBuf, getPageNo(lastPageBuf));
		}
		
		
	}


	private static ADD_STATUS _addTuple(byte [] pageBuf, byte[] tuple) {
		
		TupleIterator iter = getTupleIterator(pageBuf);
		
		while(null != iter.getNext()) { 
			
		}
		
		int [] tuple_start = pageOffset.get(PAGE_ITEMS.TUPLE_START);
		int indx = tuple_start[0] + iter.curPos * Tuple.TupleSize();
		
		int next_page = getNextPage(pageBuf);
		int no_of_tuples = getNoOfTuples(pageBuf);
		
		if (-1 == next_page && Page.MAX_TUPLES <= no_of_tuples) return ADD_STATUS.PAGE_FULL;
		
		setTuple(pageBuf, indx, tuple);
		no_of_tuples += 1;
		setNoOfTuples(pageBuf, no_of_tuples);
		
		return ADD_STATUS.SUCCESS;
	}
	
	public static TupleIterator getTupleIterator(byte [] buff) {	
		return new Page().new TupleIterator(buff);
	}
	
	private class TupleIterator{	
		int curPos = 0; // current position is relative, you should use tuple.start and tuple.size to fix the current location
		byte [] pageBuf;
		public TupleIterator(byte[] page) {
			pageBuf = page;
		}
		
		
		public byte[] getNextInPage() {
			int no_of_tuples = getNoOfTuples(pageBuf);
			if (curPos >= no_of_tuples) {
				/*
				 * reached end of page
				 */
				return null;
			}
			byte [] tuple = new byte[Tuple.TupleSize()];
			int indx =  pageOffset.get(PAGE_ITEMS.TUPLE_START)[0];
			indx +=  (curPos * Tuple.TupleSize());
			tuple = getTuple(pageBuf, indx);
			curPos += 1;
			return tuple;
		}
		
		
		public byte[] getNext() {
			int no_of_tuples = getNoOfTuples(pageBuf);
			if (curPos >= no_of_tuples) {
				/*
				 * check if there is a next page
				 */
				int next_page_no = getNextPage(pageBuf);
				if (-1 == next_page_no) return null;
				pageBuf = new byte[Page.PAGE_SIZE];
				LinearHash.getDisk().readPage(pageBuf, next_page_no);
				curPos = 0;
				
			}
			byte [] tuple = new byte[Tuple.TupleSize()];
			int indx =  pageOffset.get(PAGE_ITEMS.TUPLE_START)[0];
			indx +=  (curPos * Tuple.TupleSize());
			tuple = getTuple(pageBuf, indx);
			curPos += 1;
			return tuple;
		}
		
	}
	
	public static byte [] SearchTuple(byte[] firstPageBuf, byte[] key) {
		byte[] tuple = null;
		TupleIterator iter = getTupleIterator(firstPageBuf);
		
		System.out.println(new String(key) + " == ");
		byte [] nextTuple = iter.getNext();
		while(null != nextTuple ) {
			
			System.out.println(new String(Tuple.readKey(nextTuple)));
			
			if(Tuple.equals(Tuple.readKey(nextTuple), key)) {
				return nextTuple;
			}
			
			nextTuple = iter.getNext();
		}
		
		return tuple;
	}
	
	/*
	public static void main(String args []) {
		byte [] page = new byte[1024];
		setPageLen(page, 1024);
		int pg_len = getPageLen(page);
		System.out.println(pg_len);
		
		System.out.println(Double.BYTES);
	}
	
	*/
	
}
