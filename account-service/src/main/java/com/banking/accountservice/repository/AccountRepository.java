package com.banking.accountservice.repository;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.entity.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.Optional;

@Repository
public interface AccountRepository extends JpaRepository<Account, String> {
     boolean existsByEmail(String email);
     boolean existsByAccountNumber(String accountNumber);

    Optional<Account> findByAccountNumber(String accountNumber);

    Optional<BigDecimal> findBalanceByAccountNumber(String accountNumber);
}
