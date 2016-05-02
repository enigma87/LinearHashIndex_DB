import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Tuple {

	public static final Map<String,ArrayList<TupleAttribute>> tupleAttr =  new HashMap<String,ArrayList<TupleAttribute>>();
	
	
	static {
		/*
		 * go throught the xml file
		 * 	and create attributes to the tuple
		 * 	as you walk down
		 */
		DocumentBuilderFactory xmlBuildFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder xmlBuilder;
		Document xmlDoc;
		
		try {
			xmlBuilder = xmlBuildFactory.newDocumentBuilder();
			xmlDoc = xmlBuilder.parse(new File(Constant.TUPLE_CONFIG_XML));
			Element docEl = xmlDoc.getDocumentElement();
			docEl.normalize();
			NodeList nl = docEl.getChildNodes();
			
			for (int i =0 ; i < nl.getLength(); i++) {
				Node node = nl.item(i);
				if (Node.ELEMENT_NODE == node.getNodeType()){ 
					String tableName = node.getAttributes().getNamedItem("tablename").getNodeValue();
					tupleAttr.put(tableName, new ArrayList<TupleAttribute>());
					NodeList childNodes = node.getChildNodes();
					for (int j = 0; j <childNodes.getLength(); j++) {
						Node col = childNodes.item(j);
						if (Node.ELEMENT_NODE == col.getNodeType()) {
							NamedNodeMap colMap = col.getAttributes();
							String colName = colMap.getNamedItem("name").getNodeValue();
							Boolean isKey = "true".equals(colMap.getNamedItem("key").getNodeValue());
							int size = Integer.parseInt(colMap.getNamedItem("size").getNodeValue());
							tupleAttr.get(tableName).add(new TupleAttribute(isKey, size, colName));
						}	
					}
				}
			}		
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	}	
	
	/*
	 * IMPORTANT : we only write to and read (key & name) from a copy of the tuple from buffer.
	 * we should again write the tuple back to buffer ( and then back to writePage)
	 */
	
	public static int hash(byte[] inp) {
		
		final int PRIME = 907;
		
		int asciisum = 0;
		
		for (byte b : inp) {
			asciisum += Byte.toUnsignedInt(b);
		}
		//System.out.println("\t\t\t" + asciisum% PRIME);
		return asciisum%PRIME;
	}

	public static boolean equals(byte[] key1, byte[] key2) {
		
		if (key1.length != key2.length) { 
		
			byte[] shorter = key1.length > key2.length ? key2 : key1;
			byte[] longer = key1.length < key2.length ? key2 : key1;
			
			for (int i =0; i < shorter.length; i++) {
				if (!new Byte(key1[i]).equals(new Byte(key2[i]))) return false;
			}
			
			for (int i = shorter.length; i < longer.length; i++) {
				if (0 != (int) longer[i]) {
					return false;
				}
			}
			return true;
		}
		
		for (int i =0; i < key1.length; i++) {
			if (!new Byte(key1[i]).equals(new Byte(key2[i]))) return false;
		}
		return true;
	}

	public static boolean LessThan(byte key1[], byte key2 []) throws UnsupportedOperationException{
		if (key1.length != key2.length) throw new UnsupportedOperationException();
		
		for (int i =0; i < key1.length ; i++) {
			if (new Byte(key1[i]).toString().compareTo((new Byte(key2[i]).toString())) > 0) {
				return false;
			}
		}
		return true;
	}
	
	public static TupleAttribute getKeyAttribute(String tableName) {
		
		for (TupleAttribute ta : tupleAttr.get(tableName)) {
			if (ta.isKey()) return ta;
		}
		return null;
	}
	
	public static byte[] readKey(String tableName, byte tuple []) {
		TupleAttribute keyAttr = getKeyAttribute(tableName);
		byte[] key = new byte[keyAttr.getSize()] ;
		System.arraycopy(tuple, keyAttr.startIndex(tupleAttr.get(tableName)), key, 0, keyAttr.getSize());
		return key;
	}
	
	public static int TupleSize(String tableName) {
		int tuple_size = 0;
		
		for (TupleAttribute ta : tupleAttr.get(tableName)) {
			tuple_size += ta.getSize();
		}	
		return tuple_size;
	}
	
}
