package com.igot.cb.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.producer.Producer;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;

import java.io.File;
import java.io.InputStream;
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
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.ResourceUtils;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
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

    @Value("${spring.kafka.certificate.topic.name}")
    private String certificate;


    @Autowired
    RestTemplate restTemplate;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    CacheService cacheService;

    @Autowired
    private Producer producer;

    @Autowired
    private ResourceLoader resourceLoader;

    @KafkaListener(topics = "${spring.kafka.cornell.topic.name}", groupId = "${spring.kafka.consumer.group.id}")
    public void enrollUpdateConsumer(ConsumerRecord<String, String> data) {
        log.info("KafkaConsumer::enrollUpdateConsumer:topic name: {} and recievedData: {}", data.topic(), data.value());
        try {
            Map<String, Object> userCourseEnrollMap = mapper.readValue(data.value(), HashMap.class);
            if (userCourseEnrollMap.containsKey("userid") && userCourseEnrollMap.get("userid") instanceof String && userCourseEnrollMap.containsKey("courseid") && userCourseEnrollMap.get("courseid") instanceof String) {
                String extCourseId = userCourseEnrollMap.get("courseid").toString();
                JsonNode result=callExtApi(extCourseId);
                String courseId=result.path("content").get("contentId").asText();
                log.info("KafkaConsumer :: enrollUpdateConsumer ::courseId from cios api {}", courseId);
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
                    Resource resource = resourceLoader.getResource("classpath:certificateTemplate.json");
                    InputStream inputStream = resource.getInputStream();
                    JsonNode jsonNode = mapper.readTree(inputStream);
                    Map<String, Object> certificateRequest = new HashMap<>();
                    certificateRequest.put("userid", userCourseEnrollMap.get("userid"));
                    certificateRequest.put("courseid", courseId);
                    certificateRequest.put("completiondate", userCourseEnrollMap.get("completedon"));
                    certificateRequest.put("providerName",result.path("content").path("contentPartner").get("contentPartnerName").asText());
                    certificateRequest.put("courseName",result.path("content").get("name").asText());
                    certificateRequest.put("courseName",result.path("content").get("name").asText());
                    certificateRequest.put("coursePosterImage",result.path("content").path("contentPartner").get("link").asText());
                    certificateRequest.put("recipientName",readUserName(userCourseEnrollMap.get("userid").toString()));
                    replacePlaceholders(jsonNode, certificateRequest);
                    producer.push(certificate, jsonNode);
                    inputStream.close();
                    log.info("KafkaConsumer::enrollUpdateConsumer:updated");
                }
            }

        } catch (Exception e) {
            log.error("Failed to read enroll Request. Message received : " + data.value(), e);
        }
    }

    private String readUserName(String userid) {
        List<String> fields = Arrays.asList("firstname","lastname"); // Assuming user_id is the column name in your table
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("id", userid);
        List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_USER,
                propertyMap,
                fields
        );
        String firstname= (String) userEnrollmentList.stream().findFirst().get().get("firstname");
        String lastname= (String) userEnrollmentList.stream().findFirst().get().get("lastname");
        String fullname=firstname;
        if(lastname!=null){
            fullname= fullname+" "+lastname;
        }
        return fullname;
    }

    private JsonNode callExtApi(String extCourseId) {
        log.info("KafkaConsumer :: callExtApi");
        String url = baseUrl + fixedUrl + extCourseId;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", token);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = mapper.valueToTree(response.getBody());
            return jsonNode;
        } else {
            throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
        }

    }

    public static Timestamp convertToTimestamp(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            Date parsedDate = dateFormat.parse(dateString);
            return new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void replacePlaceholders(JsonNode jsonNode, Map<String, Object> certificateRequest) {
        log.info("KafkaConsumer :: replacePlaceholders");
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            objectNode.fields().forEachRemaining(entry -> {
                JsonNode value = entry.getValue();
                if (value.isTextual()) {
                    String textValue = value.asText();
                    if (textValue.startsWith("${") && textValue.endsWith("}")) {
                        String placeholder = textValue.substring(2, textValue.length() - 1);
                        String replacement = getReplacementValue(placeholder, certificateRequest);
                        objectNode.put(entry.getKey(), replacement);
                    }
                } else if (value.isArray()) {
                    value.elements().forEachRemaining(element -> {
                        if (element.isObject()) {
                            replacePlaceholders(element, certificateRequest);
                        }
                    });
                } else {
                    replacePlaceholders(value, certificateRequest);
                }
            });
        }
    }

    private static String getReplacementValue(String placeholder, Map<String, Object> certificateRequest) {
        log.info("KafkaConsumer :: getReplacementValue");
        switch (placeholder) {
            case "user.id":
                return (String) certificateRequest.get("userid");
            case "course.id":
                return (String) certificateRequest.get("courseid");
            case "today.date":
                return (String) certificateRequest.get("completiondate");
            case "time.ms":
                return String.valueOf(System.currentTimeMillis());
            case "unique.id":
                return UUID.randomUUID().toString();
            case "course.name":
                return (String) certificateRequest.get("courseName");
            case "provider.name":
                return (String) certificateRequest.get("providerName");
            case "user.name":
                return (String) certificateRequest.get("recipientName");
            case "course.poster.image":
                return (String) certificateRequest.get("coursePosterImage");
            default:
                return "";
        }
    }

}
