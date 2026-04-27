package com.ko2.events.repository;

import com.ko2.events.model.AuditEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public interface AuditEntryRepository extends JpaRepository<AuditEntry, Long> {

    Page<AuditEntry> findAllByOrderByTimestampDesc(Pageable pageable);

    Page<AuditEntry> findByUsernameOrderByTimestampDesc(String username, Pageable pageable);

    Page<AuditEntry> findByEventTypeOrderByTimestampDesc(String eventType, Pageable pageable);

    Page<AuditEntry> findByTimestampBetweenOrderByTimestampDesc(
            LocalDateTime from, LocalDateTime to, Pageable pageable);
}
