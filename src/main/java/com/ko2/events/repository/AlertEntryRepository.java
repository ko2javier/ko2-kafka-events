package com.ko2.events.repository;

import com.ko2.events.model.AlertEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AlertEntryRepository extends JpaRepository<AlertEntry, Long> {

    Page<AlertEntry> findAllByOrderByDetectedAtDesc(Pageable pageable);

    Page<AlertEntry> findByUsernameOrderByDetectedAtDesc(String username, Pageable pageable);
}
