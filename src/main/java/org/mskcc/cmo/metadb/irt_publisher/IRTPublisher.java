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

package org.mskcc.cmo.metadb.irt_publisher;

import java.util.Date;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class IRTPublisher {

    private static final Log LOG = LogFactory.getLog(IRTPublisher.class);

    private static Options getOptions(String[] args) {
        Options options = new Options();
        options.addOption("h", "help", false, "shows this help document and quits.")
            .addOption("c", "cmoRequestsOnly", false,
                       "Request filter - cmoRequests only.")
            .addOption("d", "daysBack", true,
                       "Request filter - requests completed d+1 days or earlier"
                       + " (not greater than 60, default: 7).");
        return options;
    }

    private static void help(Options options, int exitStatus) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("IRTPublisher", options);
        System.exit(exitStatus);
    }

    private static JobParametersBuilder parseArgs(String[] args) throws Exception {
        Options options = IRTPublisher.getOptions(args);
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        JobParametersBuilder toReturn = new JobParametersBuilder();
        toReturn.addDate("date", new Date());
        toReturn.addLong("time", System.currentTimeMillis());
        try {
            if (commandLine.hasOption("h")) {
                help(options, 0);
            }
            if (commandLine.hasOption("d")) {
                Integer daysBack = Integer.parseInt(commandLine.getOptionValue("d"));
                if (daysBack > 60) {
                    help(options, 1);
                }
                toReturn.addString("daysBack", String.valueOf(daysBack));
            } else {
                toReturn.addString("daysBack", "7");
            }
            if (commandLine.hasOption("c")) {
                toReturn.addString("cmoRequestsOnly", "true");
            } else {
                toReturn.addString("cmoRequestsOnly", "false");
            }
        } catch (Exception e) {
            help(options, 1);
        }
        return toReturn;
    }

    private static JobExecution launchIRTPublisherJob(ConfigurableApplicationContext ctx,
                                                      JobParametersBuilder builder) throws Exception {
        JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
        Job job = ctx.getBean(IRTPublisherJobConfiguration.IRT_PUBLISHER_JOB, Job.class);
        return jobLauncher.run(job, builder.toJobParameters());
    }

    public static void main(String [] args) throws Exception {
        SpringApplication app = new SpringApplication(IRTPublisher.class);
        ConfigurableApplicationContext ctx = app.run(args);

        JobParametersBuilder builder = null;
        try {
            builder = parseArgs(args);
        } catch (Exception e) {
            help(getOptions(args), 1);
        }
        JobExecution jobExecution = launchIRTPublisherJob(ctx, builder);
        ExitStatus exitStatus = jobExecution.getExitStatus();
        if (exitStatus.equals(ExitStatus.COMPLETED)) {
            LOG.info(IRTPublisherJobConfiguration.IRT_PUBLISHER_JOB
                     + " failed with exit status: " + exitStatus);
        } else {
            LOG.info(IRTPublisherJobConfiguration.IRT_PUBLISHER_JOB
                     + " completed with exit status: " + exitStatus);
        }

        System.exit(SpringApplication.exit(ctx));
    }
}
