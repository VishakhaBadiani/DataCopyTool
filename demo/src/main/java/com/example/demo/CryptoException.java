package com.example.demo;

public class CryptoException extends Exception{
    public CryptoException() {
    }
    public CryptoException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
