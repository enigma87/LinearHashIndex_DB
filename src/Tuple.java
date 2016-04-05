import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

	
	/*public static final int TUPLE_SIZE;
	
	public static final int DNAME_SIZE = 10 ;
	
	public static final int MNAME_SIZE = 10;
*/
	public static final char NAME_PAD = '#';
	public static final List<TupleAttribute> tupleAttr =  new ArrayList<TupleAttribute>();
	
	
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
			xmlDoc = xmlBuilder.parse(new File("tuple_config.xml"));
			Element docEl = xmlDoc.getDocumentElement();
			docEl.normalize();
			NodeList nl = docEl.getChildNodes();
			
			for (int i =0 ; i < nl.getLength(); i++) {
	
				Node node = nl.item(i);
				if (Node.ELEMENT_NODE == node.getNodeType()) {
					
					
					NamedNodeMap attrMap = node.getAttributes();
					System.out.println(attrMap.getNamedItem("key").getNodeValue().toString());
					boolean key = attrMap.getNamedItem("key").getNodeValue().toString().equals("true");
					String name = attrMap.getNamedItem("name").getNodeValue().toString();
					int size = Integer.parseInt(attrMap.getNamedItem("size").getNodeValue().toString());
					tupleAttr.add(new TupleAttribute(key, size, name));
					
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
		
		if (key1.length != key2.length) return false;
		
		for (int i =0; i < key1.length; i++) {
			if (!new Byte(key1[i]).equals(new Byte(key2[i]))) return false;
		}
		return true;
	}

	
	public static TupleAttribute getKeyAttribute() {
		
		for (TupleAttribute ta : tupleAttr) {
			if (ta.isKey()) return ta;
		}
		return null;
	}
	
	public static byte[] readKey(byte tuple []) {
		TupleAttribute keyAttr = getKeyAttribute();
		byte[] key = new byte[keyAttr.getSize()] ;
		System.arraycopy(tuple, keyAttr.startIndex(tupleAttr), key, 0, keyAttr.getSize());
		return key;
	}
	
	public static int TupleSize() {
		int tuple_size = 0;
		
		for (TupleAttribute ta : tupleAttr) {
			tuple_size += ta.getSize();
		}	
		return tuple_size;
	}
	
	public static void main(String args[]) {
		
		
	}
}
