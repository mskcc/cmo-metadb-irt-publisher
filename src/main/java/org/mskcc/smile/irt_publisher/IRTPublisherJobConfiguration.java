/*
 * Copyright (c) 2021 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

package org.mskcc.smile.irt_publisher;

import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import org.mskcc.cmo.messaging.Gateway;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
@EnableAsync
@ComponentScan(basePackages = {"org.mskcc.cmo.messaging", "org.mskcc.cmo.common.*"})
public class IRTPublisherJobConfiguration {
    public static final String IRT_PUBLISHER_JOB = "irtPublisherJob";

    @Value("${chunk.interval:10}")
    private Integer chunkInterval;

    @Value("${async.thread_pool_size:5}")
    private Integer asyncThreadPoolSize;

    @Value("${async.thread_pool_max:10}")
    private Integer asyncThreadPoolMax;

    @Value("${processor.thread_pool_size:5}")
    private Integer processorThreadPoolSize;

    @Value("${processor.thread_pool_max:10}")
    private Integer processorThreadPoolMax;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private Gateway messagingGateway;

    @Bean
    public Gateway messagingGateway() throws Exception {
        messagingGateway.connect();
        return messagingGateway;
    }

    /**
     * Creates SimpleJobLauncher.
     */
    public JobLauncher getJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(getJobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    private JobRepository getJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource());
        factory.setTransactionManager(getTransactionManager());
        factory.afterPropertiesSet();
        return (JobRepository) factory.getObject();
    }

    @Value("org/springframework/batch/core/schema-drop-sqlite.sql")
    private Resource dropRepositoryTables;

    @Value("org/springframework/batch/core/schema-sqlite.sql")
    private Resource dataRepositorySchema;

    /**
     * Creates a DataSource.
     */
    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:repository.sqlite");
        return dataSource;
    }

    /**
     * Creates a DataSourceInitializer.
     */
    public DataSourceInitializer dataSourceInitializer(DataSource dataSource) throws MalformedURLException {
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();
        databasePopulator.addScript(dropRepositoryTables);
        databasePopulator.addScript(dataRepositorySchema);
        databasePopulator.setIgnoreFailedDrops(true);

        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        initializer.setDatabasePopulator(databasePopulator);
        return initializer;
    }

    private PlatformTransactionManager getTransactionManager() {
        return new ResourcelessTransactionManager();
    }


    /**
     * Publisher step.
     */
    @Bean
    public Job irtPublisherJob() {
        return jobBuilderFactory.get(IRT_PUBLISHER_JOB)
            .start(irtPublisherStep())
            .build();
    }

    /**
     * Publisher workflow.
     */
    @Bean
    public Step irtPublisherStep() {
        return stepBuilderFactory.get("irtPublisherStep")
            .listener(irtListener())
            .<String, Future<Map<String,Object>>>chunk(chunkInterval)
            .reader(irtReader())
            .processor(asyncItemProcessor())
            .writer(asyncItemWriter())
            .build();
    }

    @Bean
    public StepExecutionListener irtListener() {
        return new IRTListener();
    }

    /**
     * Reader
     */
    @Bean
    @StepScope
    public ItemStreamReader<String> irtReader() {
        return new IRTReader();
    }

    /**
     * Provides for async processing.
     */
    @Bean(name = "asyncIRTRequestThreadPoolTaskExecutor")
    @StepScope
    public ThreadPoolTaskExecutor asyncLimsRequestThreadPoolTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(asyncThreadPoolSize);
        executor.setMaxPoolSize(asyncThreadPoolMax);
        executor.initialize();
        return executor;
    }

    /**
     * Provides for async processing.
     */
    @Bean
    @StepScope
    public ItemProcessor<String, Future<Map<String, Object>>> asyncItemProcessor() {
        AsyncItemProcessor<String, Map<String, Object>> asyncItemProcessor = new AsyncItemProcessor();
        asyncItemProcessor.setTaskExecutor(processorThreadPoolTaskExecutor());
        asyncItemProcessor.setDelegate(irtProcessor());
        return asyncItemProcessor;
    }

    /**
     * Provides for async processing.
     */
    @Bean(name = "processorThreadPoolTaskExecutor")
    @StepScope
    public ThreadPoolTaskExecutor processorThreadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(processorThreadPoolSize);
        threadPoolTaskExecutor.setMaxPoolSize(processorThreadPoolMax);
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    /**
     * IRTProcessor.
     */
    @Bean
    @StepScope
    public IRTProcessor irtProcessor() {
        return new IRTProcessor();
    }

    /**
     * Writer.
     */
    @Bean
    @StepScope
    public ItemWriter<Future<Map<String, Object>>> asyncItemWriter() {
        AsyncItemWriter<Map<String, Object>> asyncItemWriter = new AsyncItemWriter();
        asyncItemWriter.setDelegate(irtWriter());
        return asyncItemWriter;
    }

    /**
     * Writer.
     */
    @Bean
    @StepScope
    public ItemStreamWriter<Map<String, Object>> irtWriter() {
        return new IRTWriter();
    }
}

