package com.igot.cb.enrollment.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.util.dto.CustomResponse;
import com.igot.cb.util.dto.CustomResponseList;

import java.util.List;
import java.util.Map;

public interface EnrollmentService {

  CustomResponse enrollUser(JsonNode userCourseEnroll, String token);

  CustomResponseList readByUserId(String token);

  CustomResponse readByUserIdAndCourseId(String courseId,String token);
}
