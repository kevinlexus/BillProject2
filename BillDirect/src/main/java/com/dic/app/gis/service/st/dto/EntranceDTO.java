package com.dic.app.gis.service.st.dto;

import com.dic.bill.model.exs.Task;


/**
 * DTO данных подъезда
 * @author lev
 *
 */
public class EntranceDTO extends BaseDTO implements PrepDTOs {

	// Конструктор
	public EntranceDTO(Task task) {
		super(task);
	}

	// Номер подъезда
	private String num;

	public String getNum() {
		return num;
	}
	public void setNum(String num) {
		this.num = num;
	}

}
