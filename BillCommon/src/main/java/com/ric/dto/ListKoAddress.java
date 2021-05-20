package com.ric.dto;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.List;

@Value
@NoArgsConstructor(force = true)
@AllArgsConstructor()
public class ListKoAddress {
    List<KoAddress> lstKoAddress;
}
