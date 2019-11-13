/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.util.Random;

import org.apache.zookeeper.common.Time;

/**
 * Implements a token-bucket based rate limiting mechanism with optional
 * probabilistic dropping inspired by the BLUE queue management algorithm [1].
 * 实现了基于令牌桶的速率限制机制，该机制具有受BLUE队列管理算法启发的可选概率丢弃功能[1]。
 *
 * The throttle provides the {@link #checkLimit(int)} method which provides
 * a binary yes/no decision.
 *
 * The core token bucket algorithm starts with an initial set of tokens based
 * on the <code>maxTokens</code> setting. Tokens are dispensed each
 * {@link #checkLimit(int)} call, which fails if there are not enough tokens to
 * satisfy a given request.
 *
 * The token bucket refills over time, providing <code>fillCount</code> tokens
 * every <code>fillTime</code> milliseconds, capping at <code>maxTokens</code>.
 *
 * This design allows the throttle to allow short bursts to pass, while still
 * capping the total number of requests per time interval.
 *
 * One issue with a pure token bucket approach for something like request or
 * connection throttling is that the wall clock arrival time of requests affects
 * the probability of a request being allowed to pass or not. Under constant
 * load this can lead to request starvation for requests that constantly arrive
 * later than the majority.
 *
 * In an attempt to combat this, this throttle can also provide probabilistic
 * dropping. This is enabled anytime <code>freezeTime</code> is set to a value
 * other than <code>-1</code>.
 *
 * The probabilistic algorithm starts with an initial drop probability of 0, and
 * adjusts this probability roughly every <code>freezeTime</code> milliseconds.
 * The first request after <code>freezeTime</code>, the algorithm checks the
 * token bucket. If the token bucket is empty, the drop probability is increased
 * by <code>dropIncrease</code> up to a maximum of <code>1</code>. Otherwise, if
 * the bucket has a token deficit less than <code>decreasePoint * maxTokens</code>,
 * the probability is decreased by <code>dropDecrease</code>.
 *
 * Given a call to {@link #checkLimit(int)}, requests are first dropped randomly
 * based on the current drop probability, and only surviving requests are then
 * checked against the token bucket.
 *
 * When under constant load, the probabilistic algorithm will adapt to a drop
 * frequency that should keep requests within the token limit. When load drops,
 * the drop probability will decrease, eventually returning to zero if possible.
 *
 * [1] "BLUE: A New Class of Active Queue Management Algorithms"
 *
 * 节流阀提供了{@link #checkLimit（int）}方法，该方法提供了二进制“是/否”决定。
 *
 * 核心令牌存储区算法从基于<code> maxTokens </ code>设置的一组初始令牌开始。每次{@link #checkLimit（int）}调用都会分配令牌，如果没有足够的令牌来满足给定请求，该调用将失败。
 *
 * 令牌桶随时间重新填充，每<code> fillTime </ code>毫秒提供一次<code> fillCount </ code>令牌，上限为<code> maxTokens </ code>。
 *
 * 这种设计允许节流阀允许短脉冲通过，同时仍限制每个时间间隔的请求总数。
 *
 * 对于诸如请求或连接限制之类的事情，纯令牌桶方法的一个问题是请求的挂钟到达时间会影响请求通过与否的可能性。在恒定负载下，这会导致请求饥饿，因为请求持续不断地晚于大多数请求到达。
 *
 * 为了解决这个问题，该节流阀还可以提供概率下降。只要<code> freezeTime </ code>设置为<code> -1 </ code>以外的值，就可以启用此功能。
 *
 * 概率算法以初始下降概率0开始，并大致每<code> freezeTime </ code>毫秒调整一次此概率。在<code> freezeTime </ code>之后的第一个请求，该算法检查令牌存储区。如果令牌存储区为空，则丢弃概率会增加<code> dropIncrease </ code>，最大值为<code> 1 </ code>。否则，如果存储桶的令牌不足量小于<code> decreasePoint * maxTokens </ code>，则概率降低<code> dropDecrease </ code>。
 *
 * 给定对{@link #checkLimit（int）}的调用后，首先会根据当前的丢弃概率随机丢弃请求，然后仅对令牌桶中尚存的请求进行检查。
 *
 * 在恒定负载下，概率算法将适应丢弃频率，该丢弃频率应将请求保持在令牌限制之内。当负载下降时，下降的可能性将降低，如果可能的话，最终将返回零。
 *
 * [1]“蓝色：一种新的主​​动队列管理算法”
 *
 *
 **/

public class BlueThrottle {
    private int maxTokens; /*可配置.如果是0,则会禁用限制*/
    private int fillTime;  /*可配置*/
    private int fillCount; /*可配置*/
    private int tokens;  /*可配置*/
    private long lastTime;

    private int freezeTime; /*可配置.如果是-1,则会禁用限制*/
    private long lastFreeze;
    private double dropIncrease; /*可配置*/
    private double dropDecrease; /*可配置*/
    private double decreasePoint; /*可配置*/
    private double drop;

    Random rng;

    public static final String CONNECTION_THROTTLE_TOKENS = "zookeeper.connection_throttle_tokens";
    public static final int DEFAULT_CONNECTION_THROTTLE_TOKENS;

    public static final String CONNECTION_THROTTLE_FILL_TIME = "zookeeper.connection_throttle_fill_time";
    public static final int DEFAULT_CONNECTION_THROTTLE_FILL_TIME;

    public static final String CONNECTION_THROTTLE_FILL_COUNT = "zookeeper.connection_throttle_fill_count";
    public static final int DEFAULT_CONNECTION_THROTTLE_FILL_COUNT;

    public static final String CONNECTION_THROTTLE_FREEZE_TIME = "zookeeper.connection_throttle_freeze_time";
    public static final int DEFAULT_CONNECTION_THROTTLE_FREEZE_TIME;

    public static final String CONNECTION_THROTTLE_DROP_INCREASE = "zookeeper.connection_throttle_drop_increase";
    public static final double DEFAULT_CONNECTION_THROTTLE_DROP_INCREASE;

    public static final String CONNECTION_THROTTLE_DROP_DECREASE = "zookeeper.connection_throttle_drop_decrease";
    public static final double DEFAULT_CONNECTION_THROTTLE_DROP_DECREASE;

    public static final String CONNECTION_THROTTLE_DECREASE_RATIO = "zookeeper.connection_throttle_decrease_ratio";
    public static final double DEFAULT_CONNECTION_THROTTLE_DECREASE_RATIO;


    static {
        DEFAULT_CONNECTION_THROTTLE_TOKENS = Integer.getInteger(CONNECTION_THROTTLE_TOKENS, 0);
        DEFAULT_CONNECTION_THROTTLE_FILL_TIME = Integer.getInteger(CONNECTION_THROTTLE_FILL_TIME, 1);
        DEFAULT_CONNECTION_THROTTLE_FILL_COUNT = Integer.getInteger(CONNECTION_THROTTLE_FILL_COUNT, 1);

        DEFAULT_CONNECTION_THROTTLE_FREEZE_TIME = Integer.getInteger(CONNECTION_THROTTLE_FREEZE_TIME, -1);
        DEFAULT_CONNECTION_THROTTLE_DROP_INCREASE = getDoubleProp(CONNECTION_THROTTLE_DROP_INCREASE, 0.02);
        DEFAULT_CONNECTION_THROTTLE_DROP_DECREASE = getDoubleProp(CONNECTION_THROTTLE_DROP_DECREASE, 0.002);
        DEFAULT_CONNECTION_THROTTLE_DECREASE_RATIO = getDoubleProp(CONNECTION_THROTTLE_DECREASE_RATIO, 0);
    }

    /* Varation of Integer.getInteger for real number properties
     * 用于实数属性的Integer.getInteger的Varation */
    private static double getDoubleProp(String name, double def) {
        String val = System.getProperty(name);
        if(val != null) {
            return Double.parseDouble(val);
        }
        else {
            return def;
        }
    }


    public BlueThrottle() {
        // Disable throttling by default 默认情况下禁用限制 (maxTokens = 0)
        this.maxTokens = DEFAULT_CONNECTION_THROTTLE_TOKENS;
        this.fillTime  = DEFAULT_CONNECTION_THROTTLE_FILL_TIME;
        this.fillCount = DEFAULT_CONNECTION_THROTTLE_FILL_COUNT;
        this.tokens = maxTokens;
        this.lastTime = Time.currentElapsedTime();

        // Disable BLUE throttling by default 默认情况下禁用BLUE限制 (freezeTime = -1)
        this.freezeTime = DEFAULT_CONNECTION_THROTTLE_FREEZE_TIME;
        this.lastFreeze = Time.currentElapsedTime();
        this.dropIncrease = DEFAULT_CONNECTION_THROTTLE_DROP_INCREASE;
        this.dropDecrease = DEFAULT_CONNECTION_THROTTLE_DROP_DECREASE;
        this.decreasePoint = DEFAULT_CONNECTION_THROTTLE_DECREASE_RATIO;
        this.drop = 0;

        this.rng = new Random();
    }

    public synchronized void setMaxTokens(int max) {
        int deficit = maxTokens - tokens;
        maxTokens = max;
        tokens = max - deficit;
    }

    public synchronized void setFillTime(int time) {
        fillTime = time;
    }

    public synchronized void setFillCount(int count) {
        fillCount = count;
    }

    public synchronized void setFreezeTime(int time) {
        freezeTime = time;
    }

    public synchronized void setDropIncrease(double increase) {
        dropIncrease = increase;
    }

    public synchronized void setDropDecrease(double decrease) {
        dropDecrease = decrease;
    }

    public synchronized void setDecreasePoint(double ratio) {
        decreasePoint = ratio;
    }

    public synchronized int getMaxTokens() {
        return maxTokens;
    }

    public synchronized int getFillTime() {
        return fillTime;
    }

    public synchronized int getFillCount() {
        return fillCount;
    }

    public synchronized int getFreezeTime() {
        return freezeTime;
    }

    public synchronized double getDropIncrease() {
        return dropIncrease;
    }

    public synchronized double getDropDecrease() {
        return dropDecrease;
    }

    public synchronized double getDecreasePoint() {
        return decreasePoint;
    }

    public synchronized double getDropChance() {
        return drop;
    }

    public synchronized int getDeficit() {
        return maxTokens - tokens;
    }

    // 检查限制
    public synchronized boolean checkLimit(int need) {
        // A maxTokens setting of zero disables throttling
        // maxTokens设置为零会禁用限制
        if (maxTokens == 0)
            return true;

        long now = Time.currentElapsedTime();
        long diff = now - lastTime;

        if (diff > fillTime) {
            int refill = (int)(diff * fillCount / fillTime);
            tokens = Math.min(tokens + refill, maxTokens);
            lastTime = now;
        }

        // A freeze time of -1 disables BLUE randomized throttling
        // 冻结时间为-1会禁用BLUE随机限制
        if(freezeTime != -1) {
            if(!checkBlue(now)) {
                return false;
            }
        }

        if (tokens < need) {
            return false;
        }

        tokens -= need;
        return true;
    }

    public synchronized boolean checkBlue(long now) {
        int length = maxTokens - tokens;
        int limit = maxTokens;
        long diff = now - lastFreeze;
        long threshold = Math.round(maxTokens * decreasePoint);

        if (diff > freezeTime) {
            if((length == limit) && (drop < 1)) {
                drop = Math.min(drop + dropIncrease, 1);
            }
            else if ((length <= threshold) && (drop > 0)) {
                drop = Math.max(drop - dropDecrease, 0);
            }
            lastFreeze = now;
        }

        if (rng.nextDouble() < drop) {
            return false;
        }
        return true;
    }
}
