package com.igot.cb.enrollment.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.enrollment.entity.CiosContentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CiosContentRepository extends JpaRepository<CiosContentEntity,String> {
    Optional<CiosContentEntity> findByContentIdAndIsActive(String contentId, boolean b);
}
