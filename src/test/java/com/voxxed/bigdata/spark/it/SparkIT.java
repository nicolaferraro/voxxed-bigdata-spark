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
package com.voxxed.bigdata.spark.it;


import java.util.List;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import com.voxxed.bigdata.spark.Event;
import com.voxxed.bigdata.spark.Recommendation;
import com.voxxed.bigdata.spark.it.support.RestAPI;

import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.JavaConversions;

import static io.fabric8.kubernetes.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Arquillian.class)
@RunAsClient
public class SparkIT {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @ArquillianResource
    KubernetesClient client;

    @Test
    public void testAppProvisionsRunningPods() throws Exception {
        assertThat(client).deployments().pods().isPodReadyForPeriod();
        assertThat(client).podsForService("voxxed-bigdata-web").runningStatus();

        // We send messages to Kafka through a rest container
        logger.info("Starting test");

        RestAPI api = new RestAPI(new DefaultKubernetesClient());

        api.addRating(new Event(1, 100, 4, 1000));
        api.addRating(new Event(1, 101, 5, 2000));
        api.addRating(new Event(2, 100, 4, 3000));

        Recommendation recommendation = api.getRecommendation(2, 600_000);

        assertNotNull(recommendation);
        List<String> coll = JavaConversions.asJavaCollection(recommendation.items()).stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(2L, recommendation.userId());
        assertTrue(coll.size() == 1);
        assertTrue(coll.contains("101"));
    }

}
