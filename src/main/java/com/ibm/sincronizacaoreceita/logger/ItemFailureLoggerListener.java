package com.ibm.sincronizacaoreceita.logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.annotation.OnReadError;
import org.springframework.batch.core.annotation.OnWriteError;
import org.springframework.batch.core.listener.ItemListenerSupport;

import java.util.List;

public class ItemFailureLoggerListener extends ItemListenerSupport {
    // TODO: Improve logger, maybe using log4j 2 as logging method

    private static final Log logger = LogFactory.getLog("Exception");

    @OnReadError
    public void onReadError(Exception ex) {
        logger.error("Erro encontrado durante a leitura do arquivo", ex);
    }

    @OnWriteError
    public void onWriteError(Exception ex, List items) {
        logger.error("Erro encontrado durante a escrita do arquivo", ex);
    }
}
