package com.igot.cb.enrollment.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.util.dto.CustomResponse;

import java.util.List;
import java.util.Map;

public interface EnrollmentService {

  CustomResponse enrollUser(JsonNode userCourseEnroll, String token);

  CustomResponse readByUserId(String id, String token);

  CustomResponse readByUserIdAndCourseId(String userId,String courseId,String token);
}
