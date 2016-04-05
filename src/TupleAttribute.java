
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
	
}
