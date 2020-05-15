/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.eureka.lease;

import com.netflix.eureka.registry.AbstractInstanceRegistry;

/**
 * Describes a time-based availability of a {@link T}. Purpose is to avoid
 * accumulation of instances in {@link AbstractInstanceRegistry} as result of ungraceful
 * shutdowns that is not uncommon in AWS environments.
 * 描述{@link T}的基于时间的可用性。
 * 目的是避免在{@link AbstractInstanceRegistry}中由于不适当的关闭而导致实例积累，这种情况在AWS环境中并不少见。
 *
 * If a lease elapses without renewals, it will eventually expire consequently
 * marking the associated {@link T} for immediate eviction - this is similar to
 * an explicit cancellation except that there is no communication between the
 * {@link T} and {@link LeaseManager}.
 * 如果租约在没有更新的情况下过期，那么它最终将过期，从而标记关联的{@link T}用于立即驱逐——这类似于显式取消，只是在{@link T}和{@link LeaseManager}之间没有通信。
 *
 * @author Karthik Ranganathan, Greg Kim
 */
public class Lease<T> {

    enum Action {
        Register, Cancel, Renew
    };

    // 默认心跳续约时间(秒)
    public static final int DEFAULT_DURATION_IN_SECS = 90;

    // 泛型，一般情况下就是咱们的服务
    private T holder;
    // 服务被剔除的时间戳
    private long evictionTimestamp;
    // 服务注册时间戳
    private long registrationTimestamp;
    // 最后一次服务正常工作的启动时间戳
    private long serviceUpTimestamp;
    // Make it volatile so that the expiration task would see this quicker
    /**
     * 最后操作时间戳
     * 但是对于这个属性的定义有歧义: 在心跳续约时这个属性存的是 当前系统时间 + 心跳续约时长 即为服务的过期时间,
     * 如果这个属性真是作为最后操作时间, 就应该存的就是当前系统时间, 不用加心跳续约时长
     *
     * 这里其实应该定义另一个新的属性来存储这个心跳续约后服务的过期时间, 也是在判断服务过期方法{@link #isExpired(long)}中提到的那个bug: 2 * duration
     */
    private volatile long lastUpdateTimestamp;
    // 心跳续约时间(毫秒)
    private long duration;

    /**
     * @param r                 服务实例
     * @param durationInSecs    心跳续约时间
     */
    public Lease(T r, int durationInSecs) {
        holder = r;
        registrationTimestamp = System.currentTimeMillis();
        lastUpdateTimestamp = registrationTimestamp;
        duration = (durationInSecs * 1000);

    }

    /**
     * 服务心跳续约
     * Renew the lease, use renewal duration if it was specified by the
     * associated {@link T} during registration, otherwise default duration is
     * {@link #DEFAULT_DURATION_IN_SECS}.
     */
    public void renew() {
        /**
         * 对于lastUpdateTimestamp这个属性的定义有歧义: 在心跳续约时这个属性存的是 当前系统时间 + 心跳续约时长 即为服务的过期时间,
         * 这里其实应该定义另一个新的属性来存储这个心跳续约后服务的过期时间, 也是在判断服务过期方法{@link #isExpired(long)}中提到的那个bug: 2 * duration
         */
        lastUpdateTimestamp = System.currentTimeMillis() + duration;
        // 如果这个属性真是作为最后操作时间, 就应该存的就是当前系统时间, 不用加心跳续约时长, 所以其实这里应该改为如下:
        // lastUpdateTimestamp = System.currentTimeMillis();

    }

    /**
     * 服务取消
     * Cancels the lease by updating the eviction time.
     */
    public void cancel() {
        // 更新服务被剔除的时间戳
        if (evictionTimestamp <= 0) {
            evictionTimestamp = System.currentTimeMillis();
        }
    }

    /**
     * 服务启动
     * Mark the service as up. This will only take affect the first time called,
     * subsequent calls will be ignored.
     */
    public void serviceUp() {
        if (serviceUpTimestamp == 0) {
            serviceUpTimestamp = System.currentTimeMillis();
        }
    }

    /**
     * Set the leases service UP timestamp.
     */
    public void setServiceUpTimestamp(long serviceUpTimestamp) {
        this.serviceUpTimestamp = serviceUpTimestamp;
    }

    /**
     * 判断是否过期
     * Checks if the lease of a given {@link com.netflix.appinfo.InstanceInfo} has expired or not.
     */
    public boolean isExpired() {
        return isExpired(0l);
    }

    /**
     * 判断是否过期
     * Checks if the lease of a given {@link com.netflix.appinfo.InstanceInfo} has expired or not.
     *
     * Note that due to renew() doing the 'wrong" thing and setting lastUpdateTimestamp to +duration more than
     * what it should be, the expiry will actually be 2 * duration. This is a minor bug and should only affect
     * instances that ungracefully shutdown. Due to possible wide ranging impact to existing usage, this will
     * not be fixed.
     * 注意，由于在 {@link #renew() } 做了“错误”的事情，并将lastUpdateTimestamp设置为+duration，超过了它应该的值，到期实际上将是2 * duration。
     * 这是一个小错误，应该只影响那些不正常关闭的实例。由于可能对现有的使用产生广泛的影响，这个问题将不会得到解决。
     *
     * @param additionalLeaseMs any additional lease time to add to the lease evaluation in ms.
     *                          在微服务中添加任何额外的租赁时间到租赁评估。
     *                          简单来说就是 集群同步的耗时, 这个时间计算不准确, 没有修复
     */
    public boolean isExpired(long additionalLeaseMs) {
        // 服务过期剔除时间戳 > 0, 表示服务已经被剔除了
        // 当前时间戳 > (最后操作时间戳 + 心跳续约时长 + 集群同步的耗时)
        return (evictionTimestamp > 0 || System.currentTimeMillis() > (lastUpdateTimestamp + duration + additionalLeaseMs));
    }

    /**
     * Gets the milliseconds since epoch when the lease was registered.
     *
     * @return the milliseconds since epoch when the lease was registered.
     */
    public long getRegistrationTimestamp() {
        return registrationTimestamp;
    }

    /**
     * Gets the milliseconds since epoch when the lease was last renewed.
     * Note that the value returned here is actually not the last lease renewal time but the renewal + duration.
     *
     * @return the milliseconds since epoch when the lease was last renewed.
     */
    public long getLastRenewalTimestamp() {
        return lastUpdateTimestamp;
    }

    /**
     * Gets the milliseconds since epoch when the lease was evicted.
     *
     * @return the milliseconds since epoch when the lease was evicted.
     */
    public long getEvictionTimestamp() {
        return evictionTimestamp;
    }

    /**
     * Gets the milliseconds since epoch when the service for the lease was marked as up.
     *
     * @return the milliseconds since epoch when the service for the lease was marked as up.
     */
    public long getServiceUpTimestamp() {
        return serviceUpTimestamp;
    }

    /**
     * Returns the holder of the lease.
     */
    public T getHolder() {
        return holder;
    }

}
