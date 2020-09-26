package com.ibm.sincronizacaoreceita.configuration;

import com.ibm.sincronizacaoreceita.domain.Account;
import com.ibm.sincronizacaoreceita.exception.FileInvalidOrNotFoundException;
import com.ibm.sincronizacaoreceita.logger.ItemFailureLoggerListener;
import com.ibm.sincronizacaoreceita.service.ReceitaService;
import com.ibm.sincronizacaoreceita.validation.Validator;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.text.NumberFormat;
import java.util.Locale;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    private final Validator validator = new Validator();

    @Bean
    @StepScope
    public FlatFileItemReader<Account> fileAccountReader(
            @Value("#{jobParameters['pathToResource']}") String pathToResource) throws FileInvalidOrNotFoundException {

        FileSystemResource resource = new FileSystemResource(pathToResource);
        if(!resource.isReadable())
            throw new FileInvalidOrNotFoundException();

        return new FlatFileItemReaderBuilder<Account>()
                .saveState(false)
                .resource(resource)
                .delimited()
                .delimiter(";")
                .names(new String[]{"agency", "accountNumber", "balance", "status"})
                .fieldSetMapper(fieldSet -> {
                    Account account = new Account();
                    try {
                        account.setAgency(fieldSet.readString("agency"));
                        account.setAccountNumber(fieldSet.readString("accountNumber"));
                        account.setBalance(fieldSet.readString("balance"));
                        account.setStatus(fieldSet.readString("status"));
                        validator.validateAccount(account);
                    } catch (Exception ex) {
                        account.setResult("erro: não foi possível ler alguma das colunas"); // TODO: extract from "ex" which column couldn't be read to set a specific error
                    }
                    return account;
                })
                .linesToSkip(1) // skipping first line which is the "header"
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<Account> writer() {
        FlatFileItemWriter<Account> writer = new FlatFileItemWriter<>(); // Create writer instance
        writer.setHeaderCallback(writer1 -> writer1.write("agencia;conta;saldo;status;resultado"));
        writer.setResource(new FileSystemResource("ContasEnviadas.csv")); // Set output file location
        writer.setLineAggregator(new DelimitedLineAggregator<>() { // Name field values sequence based on object properties
            {
                setDelimiter(";");
                setFieldExtractor(new BeanWrapperFieldExtractor<>() {
                    {
                        setNames(new String[]{"agency", "accountNumber", "balance", "status", "result"});
                    }
                });
            }
        });
        return writer;
    }

    @Bean
    public AsyncItemProcessor<Account, Account> asyncItemProcessor() {
        AsyncItemProcessor<Account, Account> processor = new AsyncItemProcessor<>();

        processor.setDelegate(processor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor()); // It makes process run on separated thread

        return processor; // It will return a future of the processing result from processor()
    }



    @Bean
    public AsyncItemWriter<Account> asyncItemWriter() {
        AsyncItemWriter<Account> writer = new AsyncItemWriter<>();

        writer.setDelegate(writer()); // It unwraps my "future" result and sends it to the writer

        return writer;
    }


    @Bean
    public ItemProcessor<Account, Account> processor() {

        return (account) -> {
            try {
                ReceitaService receitaService = new ReceitaService();
                NumberFormat format = NumberFormat.getInstance(Locale.FRANCE);
                Number parsedBalance = format.parse(account.getBalance());


                if (account.getResult() == null) {
                    if (receitaService.atualizarConta(
                            account.getAgency(),
                            account.getAccountNumber().replace("-", ""),
                            parsedBalance.doubleValue(),
                            account.getStatus())) {
                        account.setResult("Atualizado");
                    } else {
                        account.setResult("Erro");
                        // Since I already made all those validations it's almost impossible to have this error but I added just in case
                    }
                }
            } catch (Exception ex){
                account.setResult("Erro");
            }

            return account;
        };
    }


    @Bean
    public Job asyncJob() throws FileInvalidOrNotFoundException {
        return this.jobBuilderFactory.get("asyncJob")
                .start(step1async())
                .build();
    }


    @Bean
    public Step step1async() throws FileInvalidOrNotFoundException {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor(); // It launches additional threads to the step
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.afterPropertiesSet();

        return this.stepBuilderFactory.get("step1async")
                .<Account, Account>chunk(100) // chuck size really depends on the situation, benchmarking is the best way define this number
                .reader(fileAccountReader(null))
                .processor((ItemProcessor) asyncItemProcessor())
                .writer(asyncItemWriter())
//                .faultTolerant() // Enable retry
//                .retryLimit(3) // I set maximum retry count of 3
//                .retry(RuntimeException.class) // It will only retry for RuntimeException or InterruptedException and not others (these are the main exceptions that ReceitaService might throw)
//                .retry(InterruptedException.class) // TODO: retry isn't fully working yet and is causing some warnings, I need to rework it, the library spring retry might help
//                 it looks I can't use multiple threads and retry together https://stackoverflow.com/questions/23780587/spring-batch-reader-in-multi-threader-job
                .taskExecutor(taskExecutor)
                .listener(new ItemFailureLoggerListener()) // logging errors is important
                .build();
    }



}

