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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class IRTWriter implements ItemStreamWriter<Map<String, Object>> {

    @Autowired
    private Gateway messagingGateway;

    @Value("${irt.publisher_topic}")
    private String IRT_PUBLISHER_TOPIC;

    private ObjectMapper mapper = new ObjectMapper();
    private static final Log LOG = LogFactory.getLog(IRTWriter.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends Map<String, Object>> requestResponseList) throws Exception {
        for (Map<String, Object> request : requestResponseList) {
            try {
                String requestJson = mapper.writeValueAsString(request);
                LOG.info("\nPublishing IRT new status:\n\n"
                         + requestJson + "\n\n on topic: " + IRT_PUBLISHER_TOPIC);
                messagingGateway.publish(IRT_PUBLISHER_TOPIC, requestJson);
            } catch (Exception e) {
                LOG.error("Error encountered during attempt to process request ids - exiting...");
                throw new RuntimeException(e);
            }
        }
    }

}
