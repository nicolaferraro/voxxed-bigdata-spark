/*
 * Copyright 2016 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.voxxed.bigdata.spark.it.support;

import io.fabric8.kubernetes.client.KubernetesClient;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.voxxed.bigdata.spark.Event;
import com.voxxed.bigdata.spark.Recommendation;

import org.apache.http.conn.HttpHostConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class RestAPI {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private String host;

    public RestAPI(KubernetesClient client) {
        int trial = 5;
        try {
            this.host = client.services().withName("voxxed-bigdata-web").get().getSpec().getClusterIP();
            if (this.host == null) {
                throw new RuntimeException("Host is null");
            }
        } catch (Exception e) {
            trial--;
            try {
                Thread.sleep(2000);
            } catch (Exception te) {}
            if (trial <= 0) {
                throw new RuntimeException("Cannot find route to probe", e);
            }
        }
    }

    public void addRating(Event event) {
        logger.info("Adding rating: {}", event);
        HttpResponse<String> res;
        for (int i=0; i < 5; i++) {
            try {
                res = Unirest.post("http://" + host + "/api/ratings")
                        .body(event.toJson())
                        .asString();

                assertTrue(res.getStatus() >= 200 && res.getStatus() < 300);
                return;
            } catch (UnirestException e) {
                if (e.getCause() != null && e.getCause() instanceof HttpHostConnectException) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException te) {}
                }
            } catch (Exception ex) {
                throw new RuntimeException("Cannot post", ex);
            }
        }
    }

    public Recommendation getRecommendation(long userId, long timeout) { // multiple of 5 seconds
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + timeout) {
            try {
                logger.info("Polling recommendation for user {}", userId);
                HttpResponse<String> node = Unirest.get("http://" + host + "/api/recommendations/" + userId)
                        .asString();

                if (node.getStatus() == 200) {
                    Recommendation rec = Recommendation.fromJson(node.getBody());
                    logger.info("Recommendation found for user {}: {}", userId, rec);
                    return rec;
                }
            } catch (Exception ex) {
                throw new RuntimeException("Cannot get", ex);
            }

            try {
                Thread.sleep(1000);
            } catch (Exception te) {}
        }
        return null;
    }


}
