package com.igot.cb.enrollment.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.util.dto.CustomResponse;
import com.igot.cb.util.Constants;
import com.igot.cb.util.dto.CustomResponseList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/cios-enroll")
public class EnrollmentController {

  @Autowired
  private EnrollmentService enrollmentService;

  @PostMapping("/v1/create")
  public ResponseEntity<CustomResponse> create(@RequestBody JsonNode userCourseEnroll, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    CustomResponse response = enrollmentService.enrollUser(userCourseEnroll, token);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/v1/courselist/byuserid")
  public ResponseEntity<?> readByUserId(@RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    CustomResponseList response = enrollmentService.readByUserId(token);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @GetMapping("/v1/readby/useridcourseid/{courseid}")
  public ResponseEntity<?> readByUserIdAndCourseId(@PathVariable String courseid, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    CustomResponse response = enrollmentService.readByUserIdAndCourseId(courseid,token);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
}
