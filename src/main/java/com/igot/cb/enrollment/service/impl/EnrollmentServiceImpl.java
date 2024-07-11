package com.igot.cb.enrollment.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.util.dto.CustomResponse;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PayloadValidation;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import java.sql.Timestamp;
import java.util.*;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EnrollmentServiceImpl implements EnrollmentService {

  @Autowired
  private PayloadValidation payloadValidation;

  @Autowired
  private AccessTokenValidator accessTokenValidator;

  @Autowired
  private CassandraOperation cassandraOperation;

  @Autowired
  private ObjectMapper objectMapper;

  @Override
  public CustomResponse enrollUser(JsonNode userCourseEnroll, String token) {
    log.info("EnrollmentService::enrollUser:inside the method");
    CustomResponse response = new CustomResponse();
    try {
      if (userCourseEnroll.has(Constants.USER_ID_RQST) && !userCourseEnroll.get(
          Constants.USER_ID_RQST).isNull()) {
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
          response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
          response.setResponseCode(HttpStatus.BAD_REQUEST);
          return response;
        }
        if (!StringUtils.isBlank(userId) && userId.equalsIgnoreCase(
            userCourseEnroll.get(Constants.USER_ID_RQST).asText())) {
          if(userCourseEnroll.has(Constants.COURSE_ID_RQST) && !userCourseEnroll.get(
              Constants.COURSE_ID_RQST).isNull()){
            Map<String, Object> userCourseEnrollMap = new HashMap<>();
            userCourseEnrollMap.put("userid",
                userCourseEnroll.get(Constants.USER_ID_RQST).asText());
            userCourseEnrollMap.put("courseid",
                userCourseEnroll.get("courseId").asText());
            userCourseEnrollMap.put("progress",
                0);
            userCourseEnrollMap.put("status",
                0);
            userCourseEnrollMap.put("completedon",
                null);
            userCourseEnrollMap.put("completionpercentage",
                0);
            userCourseEnrollMap.put("issued_certificates",
                null);
            userCourseEnrollMap.put(Constants.ENROLLED_DATE,
                new Timestamp(System.currentTimeMillis()));
            cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_EXTERNAL_ENROLMENTS_T1, userCourseEnrollMap);
            response.setResponseCode(HttpStatus.OK);
            response.setResult(userCourseEnrollMap);
            return response;
          }else {
            response.setMessage("CourseId is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
          }

        } else {
          response.setMessage("UserId is not matching");
          response.setResponseCode(HttpStatus.BAD_REQUEST);
          return response;
        }

      } else {
        response.setMessage("UserId is mandatory");
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        return response;
      }

    }catch (Exception e){
      log.error("error while processing", e);
      throw new RuntimeException(e);
    }

  }

  @Override
  public CustomResponse readByUserId(String userid,String token) {
    log.info("EnrollmentService::readByUserId:inside the method");
    CustomResponse response = new CustomResponse();
    try {
      if (!StringUtils.isBlank(userid)) {
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
          response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
          response.setResponseCode(HttpStatus.BAD_REQUEST);
          return response;
        }
        if (userId.equalsIgnoreCase(userid)) {
          List<String> fields = Arrays.asList("userid","courseid","completedon","completionpercentage","enrolled_date","issued_certificates","progress","status"); // Assuming user_id is the column name in your table
          Map<String, Object> propertyMap = new HashMap<>();
          propertyMap.put("userid", userId);
          List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                  Constants.KEYSPACE_SUNBIRD_COURSES,
                  Constants.TABLE_USER_EXTERNAL_ENROLMENTS_T1,
                  propertyMap,
                  fields
          );
          Map<String, Object> result = new HashMap<>();
          for (Map<String, Object> enrollment : userEnrollmentList) {
            String courseId = (String) enrollment.get("courseid");
            result.put(courseId, enrollment);
          }
          response.setResponseCode(HttpStatus.OK);
          response.setResult(result);
          return response;
        } else {
          response.setMessage("UserId is not matching");
          response.setResponseCode(HttpStatus.BAD_REQUEST);
          return response;
        }

      } else {
        response.setMessage("UserId is mandatory");
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        return response;
      }

    }catch (Exception e){
      log.error("error while processing", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public CustomResponse readByUserIdAndCourseId(String userid, String courseid,String token) {
    log.info("EnrollmentService::readByUserIdAndCourseId:inside the method");
    CustomResponse response = new CustomResponse();
    try {
      if (!StringUtils.isBlank(userid)) {
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
          response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
          response.setResponseCode(HttpStatus.BAD_REQUEST);
          return response;
        }
        if (userId.equalsIgnoreCase(userid)) {
          // Construct fields list containing only userId
          List<String> fields = Arrays.asList("userid","courseid","completedon","completionpercentage","enrolled_date","issued_certificates","progress","status"); // Assuming user_id is the column name in your table

          // Construct propertyMap if necessary, for example:
          Map<String, Object> propertyMap = new HashMap<>();
          propertyMap.put("userid", userId); // Convert userId string to UUID
          propertyMap.put("courseid", courseid);
          // Fetch records based on keyspace, table, propertyMap, and fields
          List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                  Constants.KEYSPACE_SUNBIRD_COURSES,
                  Constants.TABLE_USER_EXTERNAL_ENROLMENTS_T1,
                  propertyMap,
                  fields
          );
          Map<String, Object> result = new HashMap<>();
          for (Map<String, Object> enrollment : userEnrollmentList) {
            // Assuming each enrollment map has a unique identifier as key, e.g., courseid
            String courseId = (String) enrollment.get("courseid"); // Assuming "courseid" is a key in the map
            result.put(courseId, enrollment); // Populate result map with courseId as key and enrollment map as value
          }
          if(!result.isEmpty()) {
            response.setResponseCode(HttpStatus.OK);
            response.setResult(result);
            return response;
          }else{
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.setMessage("courseId is not matching");
            return response;
          }
        } else {
          response.setMessage("UserId is not matching");
          response.setResponseCode(HttpStatus.BAD_REQUEST);
          return response;
        }

      } else {
        response.setMessage("UserId is mandatory");
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        return response;
      }

    }catch (Exception e){
      log.error("error while processing", e);
      throw new RuntimeException(e);
    }
  }
}
