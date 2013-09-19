package com.cwilliams.commerce;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class ProductView implements Serializable {

	private static final long serialVersionUID = 2143867834255338537L;
	private long entitlementID 	= 0;
	private int  productID		= 0;
	private long debitTime      = 0L;
	private int userID  		= 0;
	private String ipAddress = "";
	private Map<String, String> data;
	
	public ProductView(){
		super();
	}
	
	/**
	 * Creates a ProductView from a CSV representation, separated by semicolons
	 * Fields, in order, are:
	 *   0- entitlementID
	 *   1- productID
	 *   2- userID
	 *   3- debitTime
	 *   4- ipAddress
	 *   5- data (key=value&key2=value2...)
	 * @param csv
	 */
	public ProductView(String csv) throws IllegalArgumentException{
		String[] parts = csv.trim().split(";");
		if(parts.length == 6){
			try{
				setEntitlementID(Long.parseLong(parts[0]));
			}catch(NumberFormatException e){
				throw new IllegalArgumentException("Invalid number passed for EntitlementID to ProductView Constructor");
			}
			try{
				setProductID(Integer.parseInt(parts[1]));
			}catch(NumberFormatException e){
				throw new IllegalArgumentException("Invalid number passed for ProductID to ProductView Constructor");
			}
			try{
				setUserID(Integer.parseInt(parts[2]));
			}catch(NumberFormatException e){
				throw new IllegalArgumentException("Invalid number passed for UserID to ProductView Constructor");
			}
			try{
				setDebitTime(Long.parseLong(parts[3]));
			}catch(NumberFormatException e){
				throw new IllegalArgumentException("Invalid number passed for EntitlementID to ProductView Constructor");
			}
			setIpAddress(parts[4]);
			setData(csvToData(parts[5]));
		}else{
			throw new IllegalArgumentException("Invalid CSV string passed as ProductView constructor");
		}
	}
	
	private Map<String, String> csvToData(String csv){
		//Use a TreeMap to have the keys sorted
		Map<String, String> newData = new TreeMap<String, String>();
		String[] keyVals = csv.split("&");
		for(int i = 0; i < keyVals.length; i++){
			String[] keyVal = keyVals[i].split("=");
			if(keyVal.length == 2){
				newData.put(keyVal[0], keyVal[1]);
			}
		}
		return newData;
	}
	
	/**
	 * Returns a ProductView as a CSV representation, separated by semicolons, followed by a newline
	 * Fields, in order, are:
	 *   0- entitlementID
	 *   1- productID
	 *   2- userID
	 *   3- debitTime
	 *   4- ipAddress
	 *   5- data (key=value&key2=value2...)
	 **/
	public String toCSV(){
		StringBuilder ret = new StringBuilder();
		ret.append(getEntitlementID()).append(';')
			.append(getProductID()).append(';')
			.append(getUserID()).append(';')
			.append(getDebitTime()).append(';')
			.append(getIpAddress()).append(';')
			.append(getDataAsCSV())
			.append("\n");
					
		return ret.toString();
	}
	
	public String getDataAsCSV(){
		//TODO: sort csv by key name
		StringBuilder ret = new StringBuilder("");
		
		Iterator<Entry<String, String>> iter = getData().entrySet().iterator();
		boolean firstKey = true;
		while(iter.hasNext()){
			Entry<String, String> curEntry = iter.next();
			if(!firstKey){
				ret.append('&');
			}else{
				firstKey = false;
			}
			ret.append(curEntry.getKey()).append('=').append(curEntry.getValue());
		}
		
		return ret.toString();
	}
	
	public Map<String, String> getData(){
		if(data == null){
			//Use a TreeMap to have the keys sorted
			data = new TreeMap<String, String>();
		}
		return data;
	}
	
	public ProductView setData(Map<String, String> data){
		this.data = data;
		return this;
	}

	public ProductView setEntitlementID(long entitlementID) {
		this.entitlementID = entitlementID; 
		return this;
	}

	public ProductView setProductID(int productID) {
		this.productID = productID; 
		return this;
	}

	public ProductView setDebitTime(long debitTime) {
		this.debitTime = debitTime; 
		return this;
	}

	public ProductView setUserID(int userID) {
		this.userID = userID; 
		return this;
	}

	public long getEntitlementID() {
		return entitlementID;
	}

	public int getProductID() {
		return productID;
	}

	public long getDebitTime() {
		return debitTime;
	}

	public int getUserID() {
		return userID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (debitTime ^ (debitTime >>> 32));
		result = prime * result
				+ (int) (entitlementID ^ (entitlementID >>> 32));
		result = prime * result + productID;
		result = prime * result + getIpAddress().hashCode();
		result = prime * result + getData().hashCode();
		result = prime * result + userID;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProductView other = (ProductView) obj;
		if (debitTime != other.debitTime)
			return false;
		if (entitlementID != other.entitlementID)
			return false;
		if (productID != other.productID)
			return false;
		if (userID != other.userID)
			return false;
		if(!getData().equals(other.getData()))
			return false;
		if(!getIpAddress().equals(other.getIpAddress()))
			return false;
		return true;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

}
