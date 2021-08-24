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
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.commons.logging.Log;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

@Component
public class IRTUtil {

    @Value("${irt.base_url}")
    private String irtBaseUrl;

    @Value("${irt.username}")
    private String irtUsername;

    @Value("${irt.password}")
    private String irtPassword;

    @Value("${irt.refresh_cache_endpoint}")
    private String irtRefreshCacheEndpoint;

    @Value("${irt.request_list_endpoint}")
    private String irtRequestListEndpoint;

    @Value("${irt.request_info_endpoint}")
    private String irtRequestInfoEndpoint;

    private ObjectMapper mapper =  new ObjectMapper();
    protected Collection<String> irtErrors = Collections.synchronizedCollection(new ArrayList<>());

    /**
     * Calls into request tracker to get list of request ids.
     */
    public List<String> getRequestIds(String daysBack, Boolean cmoRequestsOnly) throws Exception {
        String requestUrl = irtBaseUrl + irtRequestListEndpoint + daysBack;
        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        ResponseEntity responseEntity =
            restTemplate.exchange(requestUrl, HttpMethod.GET, requestEntity, Object.class);
        Map<String, Object> response =
            mapper.readValue(mapper.writeValueAsString(responseEntity.getBody()), Map.class);

        String requestListJSON = mapper.writeValueAsString(response.get("data"));
        List<Map> requestListMap = mapper.readValue(requestListJSON, List.class);
        List<String> requestIds = new ArrayList<>();
        for (Map m : requestListMap) {
            String requestId = (String)m.get("requestId");
            if (cmoRequestsOnly) {
                boolean cmoRequest = (Boolean)m.get("isCmoRequest");
                if (!cmoRequest) {
                    updateIRTErrors(requestId);
                    continue;
                }
            }
            requestIds.add(requestId);
        }
        return requestIds;
    }

    /**
     * Gets request status from request tracker.
     */
    @Async("asyncIRTRequestThreadPoolTaskExecutor")
    public CompletableFuture<Map<String, Object>> getRequestInfo(String requestId) throws Exception {
        String requestUrl = irtBaseUrl + irtRequestInfoEndpoint + requestId;
        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        ResponseEntity responseEntity =
            restTemplate.exchange(requestUrl, HttpMethod.GET, requestEntity, Object.class);
        Map<String, Object> response =
            mapper.readValue(mapper.writeValueAsString(responseEntity.getBody()), Map.class);
        return CompletableFuture.completedFuture(response);
    }

    /**
     * Used to capture failed request processing.
     */
    public void updateIRTErrors(String requestId) {
        irtErrors.add(requestId);
    }

    /**
     * Used to log failed request processing.
     */
    public void logFailedRequests(Log log) {
        if (irtErrors.isEmpty()) {
            log.info("No errors to report during fetch from IRT");
        } else {
            StringBuilder builder =
                new StringBuilder("The following request statuses could not"
                                  + " be retrieved (or were filtered):\n");
            for (String requestId : irtErrors) {
                builder.append(requestId).append("\n");
            }
            builder.deleteCharAt(builder.lastIndexOf("\n"));
            log.info(builder.toString());
        }
    }

    /**
     * Refreshes IRT request cache.
     * @param daysBack
     * @throws Exception
     */
    public void refreshCache(String daysBack) throws Exception {
        String refreshCacheUrl = irtBaseUrl + irtRefreshCacheEndpoint + daysBack;
        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        ResponseEntity response = restTemplate.exchange(refreshCacheUrl, HttpMethod.GET,
                requestEntity, Object.class);
        if (!response.getStatusCode().equals(HttpStatus.OK)) {
            throw new RuntimeException("RefreshCache did not return expected status: "
                    + response.getStatusCode());
        }
    }

    private RestTemplate getRestTemplate() throws Exception {
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
        HostnameVerifier hostnameVerifier = (s, sslSession) -> true;
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
        CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }

    private HttpEntity getRequestEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(irtUsername, irtPassword);
        return new HttpEntity<Object>(headers);
    }
}
