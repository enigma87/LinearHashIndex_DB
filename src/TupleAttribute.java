import java.util.List;

public class TupleAttribute {
	boolean key;
	int size;
	String name;
	
	
	public String getName() {
		return name;
	}

	
	public void setName(String name) {
		this.name = name;
	}

	public TupleAttribute(boolean key, int size, String name) {
		// TODO Auto-generated constructor stub
		this.setKey(key);
		this.setSize(size);
		this.setName(name);
	}

	public boolean isKey() {
		return key;
	}

	public void setKey(boolean key) {
		this.key = key;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
	
	public int startIndex(List<TupleAttribute> attrList) {
		int offset = 0;
		for (int i =0; i < attrList.size(); i++) {
			if (attrList.get(i).getName() == this.getName()) break;
			offset += attrList.get(i).size;
		}
		
		return offset;
	}	
}