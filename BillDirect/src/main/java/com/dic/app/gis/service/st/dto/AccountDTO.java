package com.dic.app.gis.service.st.dto;

import com.dic.bill.model.exs.Task;


/**
 * DTO данных лицевого счета
 * @author lev
 *
 */
public class AccountDTO extends BaseDTO implements PrepDTOs {

	// Конструктор
	public AccountDTO(Task task) {
		super(task);
	}

}
