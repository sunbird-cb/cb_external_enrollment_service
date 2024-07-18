package com.igot.cb.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Component
@Slf4j
public class KafkaConsumer {
    private ObjectMapper mapper = new ObjectMapper();

    @Value("${cios.read.api.base.url}")
    private String baseUrl;

    @Value("${cios.read.api.fixed.url}")
    private String fixedUrl;

    @Value("${kong.api.auth.token}")
    private String token;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    CacheService cacheService;

    @KafkaListener(topics = "${spring.kafka.cornell.topic.name}", groupId = "${spring.kafka.consumer.group.id}")
    public void enrollUpdateConsumer(ConsumerRecord<String, String> data) {
        log.info("KafkaConsumer::enrollUpdateConsumer:topic name: {} and recievedData: {}",data.topic(),data.value());
        try {
            Map<String, Object> userCourseEnrollMap = mapper.readValue(data.value(), HashMap.class);
            if (userCourseEnrollMap.containsKey("userid") && userCourseEnrollMap.get("userid") instanceof String && userCourseEnrollMap.containsKey("courseid") && userCourseEnrollMap.get("courseid") instanceof String) {
                String extCourseId = userCourseEnrollMap.get("courseid").toString();
                String courseId = callExtApi(extCourseId);
                log.info("KafkaConsumer :: enrollUpdateConsumer ::courseId from cios api {}",courseId);
                String[] parts = ((String) userCourseEnrollMap.get("userid")).split("@");
                userCourseEnrollMap.put("userid", parts[0]);
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put("userid", userCourseEnrollMap.get("userid"));
                propertyMap.put("courseid", courseId);
                List<Map<String, Object>> listOfMasterData = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS_T1, propertyMap, null, 1);
                if (!CollectionUtils.isEmpty(listOfMasterData)) {
                    Map<String, Object> updatedMap = new HashMap<>();
                    updatedMap.put("progress",
                            100);
                    updatedMap.put("status",
                            2);
                    if (userCourseEnrollMap.containsKey("completedon") && userCourseEnrollMap.get("completedon") instanceof String) {
                        updatedMap.put("completedon", convertToTimestamp(
                                (String) userCourseEnrollMap.get("completedon")));
                        updatedMap.put("completionpercentage",
                                100);
                    }
                    cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS_T1, updatedMap, propertyMap);
                    cacheService.deleteCache(userCourseEnrollMap.get("userid").toString() + courseId);
                    cacheService.deleteCache(userCourseEnrollMap.get("userid").toString());
                    log.info("KafkaConsumer::enrollUpdateConsumer:updated");
                }
            }

        } catch (Exception e) {
            log.error("Failed to read enroll Request. Message received : " + data.value(), e);
        }
    }

    private String callExtApi(String extCourseId) {
        String url = baseUrl + fixedUrl + extCourseId;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "bearer " + token);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = mapper.valueToTree(response.getBody());
            return jsonNode.path("content").path("contentId").asText();
        }else {
            throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
        }

    }

    public static Timestamp convertToTimestamp(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC")); // Set desired timezone, UTC in this example
        try {
            // Parse the date string into a Date object
            Date parsedDate = dateFormat.parse(dateString);
            // Convert the Date object to a Timestamp object
            return new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            // Handle the exception or rethrow it as needed
            return null;
        }
    }
}
