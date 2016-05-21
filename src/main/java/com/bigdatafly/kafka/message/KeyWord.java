/**
 * 
 */
package com.bigdatafly.kafka.message;

import java.util.Date;

/**
 * @author summer
 * 消息体
 * ID
 * UserID
 * KeyWord
 * Date
 */
public class KeyWord implements java.io.Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8301248806301009729L;

	Long id;
	String userId;
	String keyWord;
	Date current;

	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getKeyWord() {
		return keyWord;
	}
	public void setKeyWord(String keyWord) {
		this.keyWord = keyWord;
	}
	public Date getCurrent() {
		return current;
	}
	public void setCurrent(Date dateTime) {
		this.current = dateTime;
	}
	
	@Override
	public String toString(){
		
		StringBuffer sb = new StringBuffer();
		sb.append("[info kafka producer:]");
		sb.append(this.id);
		sb.append("-");
		sb.append(this.userId);
		sb.append("-");
		sb.append(this.keyWord);
		sb.append("-" + this.getCurrent());
		return sb.toString();
	}

}
