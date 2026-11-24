package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
public class AccountService {

    @Autowired
    private AccountRepository accountRepository;

    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("creating account for customer: {}", request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())) {
            log.error("Account already exists for email: {}", request.getEmail());
            throw new RuntimeException("Account already exists for email: " + request.getEmail());
        }

        Account account = Account.builder()
                .accountHolderName(request.getAccountHolderName())
                .email(request.getEmail())
                .phoneNumber(request.getPhoneNumber())
                .accountType(request.getAccountType())
                .balance(request.getInitialDeposit())
                .accountStatus(AccountStatus.ACTIVE)
                .accountNumber(generateAccountNumber())
                .dailyTransactionLimit(
                        request.getAccountType() == AccountType.SAVINGS
                                ? new BigDecimal("100000.00") : new BigDecimal("500000.00")
                )
                .build();
        Account saveAccount = accountRepository.save(account);
        log.info("Account created successfully with accountNumber : {}", saveAccount.getAccountNumber());
        return mapToAccountResponse(saveAccount);
    }

    private String generateAccountNumber() {
        String accountNumber;
        do {
            long randomNumber = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", randomNumber);
        }while(accountRepository.existsByAccountNumber(accountNumber));
        return accountNumber;
    }

    private AccountResponse mapToAccountResponse(Account saveAccount) {
       AccountResponse response = new AccountResponse();
       response.setId(saveAccount.getId());
       response.setAccountNumber(saveAccount.getAccountNumber());
       response.setAccountHolderName(saveAccount.getAccountHolderName());
       response.setEmail(saveAccount.getEmail());
       response.setPhoneNumber(saveAccount.getPhoneNumber());
       response.setAccountStatus(saveAccount.getAccountStatus());
       response.setAccountType(saveAccount.getAccountType());
       response.setBalance(saveAccount.getBalance());
       response.setDailyTransactionLimit(saveAccount.getDailyTransactionLimit().toString());
       response.setCreatedAt(saveAccount.getCreatedAt());
       return response;
    }

    public AccountResponse getAccount(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for account number: " + accountNumber));
        return mapToAccountResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for account number: " + accountNumber));
        return account.getBalance();
    }


    /**
     * Block the account by setting its status to BLOCKED
     * called by fraud detection service via kafka consumer when fraud is detected
     * @param accountNumber
     */
    public void blockAccount(String accountNumber) {
        log.info("Blocking account with accountNumber: {}", accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for account number: " + accountNumber));
        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account with accountNumber: {} has been blocked", accountNumber);
    }

    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting balance {} from account with accountNumber: {}",amount, accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for account number: " + accountNumber));
        if(account.getAccountStatus() != AccountStatus.ACTIVE) {
            log.error("Account with accountNumber: {} is not active. Current status: {}", accountNumber, account.getAccountStatus());
            throw new RuntimeException("Account is not active. account number :  " + accountNumber + ". Current status: " + account.getAccountStatus());
        }

        if(account.getBalance().compareTo(amount) < 0){
            log.error("Insufficient balance in account number: {}. Current balance: {}, attempted deduction: {}", accountNumber, account.getBalance(), amount);
            throw new RuntimeException("Insufficient balance in account number: " + accountNumber);
        }
        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance deducted successfully. New balance for accountNumber: {} is {}", accountNumber, account.getBalance());
    }

    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting balance {} to account {}", amount, accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for account number: "+accountNumber));

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Balance credited successfully. New balance for accountNumber: {} is {}", accountNumber, account.getBalance());

    }
}
