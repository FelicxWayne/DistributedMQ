package com.DBMQ.DistributedMQ.controller;

import com.DBMQ.DistributedMQ.service.DlqInspectionService;
import com.DBMQ.DistributedMQ.service.DlqReplayService;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dlq")
public class DlqController {

    private final DlqInspectionService dlqInspectionService;
    private final DlqReplayService dlqReplayService;

    public DlqController(DlqInspectionService dlqInspectionService, DlqReplayService dlqReplayService) {
        this.dlqInspectionService = dlqInspectionService;
        this.dlqReplayService = dlqReplayService;
    }

    @GetMapping
    public ResponseEntity<Object> getDlqMessages(
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        try {
            List<MapRecord<String, Object, Object>> records = dlqInspectionService.readDlqMessages(limit);
            List<Map<String, Object>> response = new ArrayList<>();
            for (MapRecord<String, Object, Object> record : records) {
                Map<String, Object> item = new HashMap<>();
                item.put("messageId", record.getId().getValue());
                item.put("payload", record.getValue());
                response.add(item);
            }
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        } catch (Exception e) {
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping("/{messageId}/replay")
    public ResponseEntity<Map<String, String>> replayMessage(@PathVariable("messageId") String messageId) {
        Map<String, String> response = new HashMap<>();
        try {
            RecordId newRecordId = dlqReplayService.replayMessage(messageId);
            response.put("status", "SUCCESS");
            response.put("message", "Message was successfully replayed.");
            response.put("originalMessageId", messageId);
            response.put("newRecordId", newRecordId.getValue());
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            response.put("status", "ERROR");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        } catch (IllegalStateException e) {
            response.put("status", "ERROR");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        } catch (Exception e) {
            response.put("status", "ERROR");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
}
