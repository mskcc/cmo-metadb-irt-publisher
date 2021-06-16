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

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class IRTReader implements ItemStreamReader<String> {

    private static final Log LOG = LogFactory.getLog(IRTReader.class);

    @Value("#{jobParameters[daysBack]}")
    private String daysBack;

    @Value("#{jobParameters[cmoRequestsOnly]}")
    private Boolean cmoRequestsOnly;

    @Autowired
    private IRTUtil irtUtil;

    private List<String> requestIdsList;

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        LOG.info("Fetching request ids from IRT going back: " + daysBack + " days.");
        try {
            this.requestIdsList = irtUtil.getRequestIds(daysBack, cmoRequestsOnly);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public String read() throws Exception, UnexpectedInputException, ParseException,
            NonTransientResourceException {
        if (!requestIdsList.isEmpty()) {
            return requestIdsList.remove(0);
        }
        return null;
    }

}
