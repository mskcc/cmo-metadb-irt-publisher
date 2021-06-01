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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

public class IRTProcessor implements ItemProcessor<String, Map<String, Object>> {
    @Autowired
    private IRTUtil irtUtil;

    @Override
    public Map<String, Object> process(String requestId) throws Exception {
        CompletableFuture<Map<String, Object>> futureRequestResponse =
            irtUtil.getRequestInfo(requestId);

        Map<String, Object> requestResponse = futureRequestResponse.get();
        if (requestResponse == null) {
            irtUtil.updateIRTErrors(requestId);
        }
        return requestResponse;
    }
}
