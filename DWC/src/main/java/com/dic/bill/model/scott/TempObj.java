package com.dic.bill.model.scott;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


//Класс строки из temp_obj, для хранения id любых объектов, для любых нужд

@Entity
@Table(name = "TEMP_OBJ")
@Getter
@Setter
public class TempObj {

	@Id
	@Column(name = "ID", unique = true, nullable = false)
	private int id;

}