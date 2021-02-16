package it.ness.api.service;

import javax.ws.rs.core.Response;

public class ReactiveException extends RuntimeException {

    public String message;
    public Response.Status status;

    public ReactiveException(String message, Response.Status status) {
        this.message = message;
        this.status = status;
    }
}
