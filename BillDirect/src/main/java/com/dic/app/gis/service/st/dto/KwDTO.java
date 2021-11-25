package com.dic.app.gis.service.st.dto;

import com.dic.bill.model.exs.Task;


/**
 * DTO данных помещения
 * @author lev
 *
 */
public class KwDTO extends BaseDTO implements PrepDTOs {

	// Конструктор
	public KwDTO(Task task) {
		super(task);
	}

	// Номер помещения
	private String num;
	// Номер подъезда
	private String entrNum;

	public String getNum() {
		return num;
	}
	public void setNum(String num) {
		this.num = num;
	}
	public String getEntrNum() {
		return entrNum;
	}
	public void setEntrNum(String entrNum) {
		this.entrNum = entrNum;
	}

}
