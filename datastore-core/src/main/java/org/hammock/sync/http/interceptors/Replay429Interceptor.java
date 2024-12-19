/*
 * Copyright © 2016, 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.hammock.sync.http.interceptors;

import org.hammock.sync.http.HttpConnectionInterceptorContext;
import org.hammock.sync.http.HttpConnectionResponseInterceptor;
import org.hammock.sync.http.internal.Utils;
import org.hammock.sync.http.internal.interceptors.HttpConnectionInterceptorException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * An implementation of {@link HttpConnectionResponseInterceptor} that retries requests if they
 * receive a 429 Too Many Requests response. The interceptor will replay the request after a delay
 * and thereafter continue to replay the request after doubling the delay time for each
 * subsequent 429 response received up to the maximum number of retries.
 */
public class Replay429Interceptor implements HttpConnectionResponseInterceptor {

    /**
     * Get an instance of a Replay429Interceptor configured with the defaults of 3 retries starting
     * at a 250 ms backoff and preferring any Retry-After header that may be sent by the server.
     */
    public static final Replay429Interceptor WITH_DEFAULTS = new Replay429Interceptor(3, 250l);

    private static final String ATTEMPT = "attempt";
    // Set a Retry-After cap of one hour
    private static final long RETRY_AFTER_CAP = TimeUnit.HOURS.toMillis(1);
    private static final Logger logger = Logger.getLogger(Replay429Interceptor.class.getName());

    private final long initialSleep;
    private final int numberOfReplays;
    private final boolean preferRetryAfter;

    /**
     * Construct a new Replay429Interceptor with a customized number of retries and initial
     * backoff time. Instances created with this constructor will honour Retry-After headers sent by
     * the server if available.
     *
     * @param numberOfReplays number of times to replay a request that received a 429
     * @param initialBackoff  the initial delay before retrying
     */
    public Replay429Interceptor(int numberOfReplays, long initialBackoff) {
        this(numberOfReplays, initialBackoff, true);
    }

    /**
     * Construct a new Replay429Interceptor with a customized number of retries and initial
     * backoff time, specifying whether to honour Retry-After headers sent from the server.
     *
     * @param numberOfReplays  number of times to replay a request that received a 429
     * @param initialBackoff   the initial delay before retrying
     * @param preferRetryAfter whether the replay should honour the duration specified by a
     *                         Retry-After header sent by the server in preference to the local
     *                         doubling backoff.
     */
    public Replay429Interceptor(int numberOfReplays, long initialBackoff, boolean
            preferRetryAfter) {
        this.numberOfReplays = numberOfReplays;
        this.initialSleep = initialBackoff;
        this.preferRetryAfter = preferRetryAfter;
    }

    @Override
    public HttpConnectionInterceptorContext interceptResponse(HttpConnectionInterceptorContext
                                                                      context) {
        try {
            HttpURLConnection urlConnection = context.connection.getConnection();
            int code = urlConnection.getResponseCode();
    
            if (code != 429) {
                return context;
            }
    
            return handle429Response(context, urlConnection);
    
        } catch (IOException e) {
            throw new HttpConnectionInterceptorException(e);
        }
    }
    
    private HttpConnectionInterceptorContext handle429Response(HttpConnectionInterceptorContext context, HttpURLConnection urlConnection) throws IOException {
        AtomicInteger attemptCounter = context.getState(this, ATTEMPT, AtomicInteger.class);
    
        if (attemptCounter == null) {
            context.setState(this, ATTEMPT, (attemptCounter = new AtomicInteger()));
        }
    
        int attempt = attemptCounter.getAndIncrement();
    
        if (attempt < numberOfReplays && context.connection.getNumberOfRetriesRemaining() > 0) {
            long sleepTime = calculateSleepTime(urlConnection, attempt);
    
            String errorString = Utils.collectAndCloseStream(urlConnection.getErrorStream());
            logger.warning(errorString + " will retry in " + sleepTime + " ms");
            logger.fine("Too many requests backing off for " + sleepTime + " ms.");
    
            sleepThread(sleepTime);
    
            context.replayRequest = true;
            return context;
        } else {
            return context;
        }
    }
    
    private long calculateSleepTime(HttpURLConnection urlConnection, int attempt) {
        long sleepTime = initialSleep * Math.round(Math.pow(2, attempt));
    
        String retryAfter = preferRetryAfter ? urlConnection.getHeaderField("Retry-After") : null;
        if (retryAfter != null) {
            try {
                sleepTime = Long.parseLong(retryAfter) * 1000;
                if (sleepTime > RETRY_AFTER_CAP) {
                    sleepTime = RETRY_AFTER_CAP;
                    logger.severe("Server specified Retry-After value in excess of one hour, capping retry.");
                }
            } catch (NumberFormatException nfe) {
                logger.warning("Invalid Retry-After value from server falling back to default backoff.");
            }
        }
        return sleepTime;
    }
    
    private void sleepThread(long sleepTime) {
        try {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
        } catch (InterruptedException e) {
            logger.fine("Interrupted during 429 backoff wait.");
        }
    }
}
