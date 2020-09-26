package com.ibm.sincronizacaoreceita.exception;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileInvalidOrNotFoundException extends Exception {

    private static final Log logger = LogFactory.getLog("Exception");

    public FileInvalidOrNotFoundException() {
        logger.error("Erro: arquivo CSV de entrada invalido ou não encontrado");
    }

}
