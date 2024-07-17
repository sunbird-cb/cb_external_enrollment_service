package com.igot.cb.util.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.http.HttpStatus;
import java.util.ArrayList;
import java.util.List;



@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CustomResponseList {
    private String message;
    private RespParam params;
    private HttpStatus responseCode;
    private List<Object> result =new ArrayList<>();

    public RespParam getParams() {
        if (params == null) {
            params = new RespParam();
        }
        return params;
    }
}
