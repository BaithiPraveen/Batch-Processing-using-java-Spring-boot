package com.batchpoc.config;

import com.batchpoc.entity.User;
import com.batchpoc.processer.UserBatchProcessor;
import com.batchpoc.repository.UserRepository;
import jakarta.persistence.EntityManagerFactory;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.io.Writer;
@Slf4j
@Configuration
@AllArgsConstructor
public class UserBatchConfig {

    private UserRepository userRepository;
    private JobRepository jobRepository;
    private PlatformTransactionManager transactionManager;
    private EntityManagerFactory entityManagerFactory;


    //read
    @Bean
    public FlatFileItemReader<User> itemReader(){
        FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();

        flatFileItemReader.setResource(new FileSystemResource("C:\\Users\\pbaithi\\Downloads\\peopledata.csv"));
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setName("csv-reader");
        flatFileItemReader.setLineMapper(lineMapper());
        log.info("i am in itemReader method for db to csv");
        return flatFileItemReader;
    }

    private LineMapper<User> lineMapper() {
        DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "userId", "firstName", "lastName", "sex", "email", "phone", "dateOfBirth", "jobTitle");

        BeanWrapperFieldSetMapper<User> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(User.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(mapper);
        log.info("i am in lineMapper method for db to csv");

        return lineMapper;
    }

    //process
    @Bean
    public UserBatchProcessor itemProcessor(){
        return new UserBatchProcessor();
    }
    //write
    @Bean
    public RepositoryItemWriter<User> itemWriter(){
        RepositoryItemWriter<User> itemWriter = new RepositoryItemWriter<>();
        itemWriter.setRepository(userRepository);
        itemWriter.setMethodName("save");
        log.info("i am in itemWriter method for db to csv");

        return itemWriter;
    }

    //step
    @Bean
    public Step step(){
        log.info("i am in step method for db to csv");

        return new StepBuilder("step in cvs to db", jobRepository)
                .<User,User>chunk(10,transactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    private TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }
    //for DB TO CSV
    //reader
    @Bean
    public JpaPagingItemReader<User> itemReaderForDbToCsv() {
        JpaPagingItemReader<User> userItemReader = new JpaPagingItemReaderBuilder<User>()
                .name("userItemReader in DB to CSV")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT u FROM User u ORDER BY u.id ASC")
                .pageSize(10)
                .build();
        log.info("data : {} ",userItemReader);
        return userItemReader;
    }

    //for DB TO CSV
    //writer
    @Bean
    public FlatFileItemWriter<User> itemWriterForDbToCsv() {
        FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
        writer.setName("userItemWriter in DB to CSV");
        writer.setResource(new FileSystemResource("D:\\batch-poc\\batch-poc\\src\\main\\resources\\data.csv"));
        writer.setHeaderCallback(headerCallback());
        writer.setLineAggregator(
                new DelimitedLineAggregator<User>() {{
                    setDelimiter(",");
                    setFieldExtractor(new BeanWrapperFieldExtractor<User>() {{
                        setNames(new String[]{"id", "userId", "firstName", "lastName", "sex", "email", "phone", "dateOfBirth", "jobTitle"});
                    }});
                }});
        return writer;
    }

    //for DB TO CSV
    //headers
    private FlatFileHeaderCallback headerCallback() {
        return new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("id,User Id,First Name,Last Name,Sex,Email,Phone,Date of Birth,Job Title");
            }
        };
    }

    //for DB TO CSV
    // Step
    @Bean
    public Step stepForDbToCsv() {
        return new StepBuilder("step in DB to CSV", jobRepository)
                .<User, User>chunk(1, transactionManager)
                .reader(itemReaderForDbToCsv())
                .processor(itemProcessor())
                .writer(itemWriterForDbToCsv())
                .taskExecutor(taskExecutor())
                .build();
    }
    //job
    @Bean
    public Job job(){
        return new JobBuilder("user-job",jobRepository)
                .start(step())
                .next(stepForDbToCsv())
                .build();
    }
}
