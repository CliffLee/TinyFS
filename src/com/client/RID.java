package com.client;

public class RID {
	public String macAddress;
	public int key;
	
	public RID() {
		macAddress = "somethingstupid";
		key = 0;
	}

	public String resolve() {
		return macAddress + String.valueOf(this.key);
	}
}
