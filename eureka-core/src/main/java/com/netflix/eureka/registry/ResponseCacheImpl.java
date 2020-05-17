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

package com.netflix.eureka.registry;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.cache.*;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.netflix.appinfo.EurekaAccept;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.converters.wrappers.EncoderWrapper;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.eureka.DefaultEurekaServerConfig;
import com.netflix.eureka.EurekaServerConfig;
import com.netflix.eureka.Version;
import com.netflix.eureka.resources.CurrentRequestVersion;
import com.netflix.eureka.resources.ServerCodecs;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that is responsible for caching registry information that will be
 * queried by the clients.
 *
 * <p>
 * The cache is maintained in compressed and non-compressed form for three
 * categories of requests - all applications, delta changes and for individual
 * applications. The compressed form is probably the most efficient in terms of
 * network traffic especially when querying all applications.
 *
 * The cache also maintains separate pay load for <em>JSON</em> and <em>XML</em>
 * formats and for multiple versions too.
 * </p>
 *
 * @author Karthik Ranganathan, Greg Kim
 */
public class ResponseCacheImpl implements ResponseCache {

    private static final Logger logger = LoggerFactory.getLogger(ResponseCacheImpl.class);

    // 全量
    public static final String ALL_APPS = "ALL_APPS";
    // 增量
    public static final String ALL_APPS_DELTA = "ALL_APPS_DELTA";

    // FIXME deprecated, here for backwards compatibility.
    private static final AtomicLong versionDeltaLegacy = new AtomicLong(0);
    private static final AtomicLong versionDeltaWithRegionsLegacy = new AtomicLong(0);

    private static final String EMPTY_PAYLOAD = "";
    private final java.util.Timer timer = new java.util.Timer("Eureka-CacheFillTimer", true);
    private final AtomicLong versionDelta = new AtomicLong(0);
    private final AtomicLong versionDeltaWithRegions = new AtomicLong(0);

    private final Timer serializeAllAppsTimer = Monitors.newTimer("serialize-all");
    private final Timer serializeDeltaAppsTimer = Monitors.newTimer("serialize-all-delta");
    private final Timer serializeAllAppsWithRemoteRegionTimer = Monitors.newTimer("serialize-all_remote_region");
    private final Timer serializeDeltaAppsWithRemoteRegionTimer = Monitors.newTimer("serialize-all-delta_remote_region");
    private final Timer serializeOneApptimer = Monitors.newTimer("serialize-one");
    private final Timer serializeViptimer = Monitors.newTimer("serialize-one-vip");
    private final Timer compressPayloadTimer = Monitors.newTimer("compress-payload");

    /**
     * This map holds mapping of keys without regions to a list of keys with region (provided by clients)
     * Since, during invalidation, triggered by a change in registry for local region, we do not know the regions
     * requested by clients, we use this mapping to get all the keys with regions to be invalidated.
     * If we do not do this, any cached user requests containing region keys will not be invalidated and will stick
     * around till expiry. Github issue: https://github.com/Netflix/eureka/issues/118
     */
    private final Multimap<Key, Key> regionSpecificKeys =
            Multimaps.newListMultimap(new ConcurrentHashMap<Key, Collection<Key>>(), new Supplier<List<Key>>() {
                @Override
                public List<Key> get() {
                    return new CopyOnWriteArrayList<Key>();
                }
            });


    /**
     * 三级缓存:
     * 三级缓存架构设计原理:
     * 首先为了提高eureka的性能, 在eureka中读操作要远大于写操作, 而读写操作不区分的话, 很容易造成读写冲突, 所以通过这种方式的读写分离, 能大大降低读写冲突.
     *
     * 获取流程: 首先进入只读缓存, 没找到进入读写缓存, 读写缓存注册了监听器, 如果还是没有取到, 就执行监听器的逻辑, 从真实数据中拿
     *
     * 缓存延迟: 只读缓存计时器延迟30s + eureka客户端(可以理解为业务微服务)缓存延迟30s + ribbon负载均衡缓存的本地注册表更新延迟60s = 120s
     *
     * 修改:
     * 只读缓存:
     *      提升效率, 保证eureka高可用的关键, 但是不保证强一致性, 只读缓存的延迟上面有介绍
     *      如果要保证强一致性, 只读缓存可以关闭, 但是关闭后性能还不如用zk
     *      数据只会来源于读写缓存, 不能主动更新, 只能通过定时器每30s从读写缓存中拿取
     *      定时任务去读写缓存拿, eureka延时原因之一
     * 读写缓存:guava
     *      读写分离, 写并发加锁, 为了给真实数据降压
     * 真实数据:内存
     *      也就是之前提到的已注册的微服务缓存池: {@link AbstractInstanceRegistry#registry}
     */

    /**
     * 只读缓存: 只能通过定时器每30s从读写缓存中拿取
     */
    private final ConcurrentMap<Key, Value> readOnlyCacheMap = new ConcurrentHashMap<Key, Value>();

    /**
     * guava 读写缓存: 为了给真实数据降压
     */
    private final LoadingCache<Key, Value> readWriteCacheMap;
    /**
     * 是否使用只读缓存, 默认为true
     */
    private final boolean shouldUseReadOnlyResponseCache;
    private final AbstractInstanceRegistry registry;
    private final EurekaServerConfig serverConfig;
    private final ServerCodecs serverCodecs;

    ResponseCacheImpl(EurekaServerConfig serverConfig, ServerCodecs serverCodecs, AbstractInstanceRegistry registry) {
        this.serverConfig = serverConfig;
        this.serverCodecs = serverCodecs;
        /**
         * 是否使用只读缓存, 默认为true
         * @see DefaultEurekaServerConfig#shouldUseReadOnlyResponseCache()
         */
        this.shouldUseReadOnlyResponseCache = serverConfig.shouldUseReadOnlyResponseCache();
        this.registry = registry;

        long responseCacheUpdateIntervalMs = serverConfig.getResponseCacheUpdateIntervalMs();

        // 读写缓存初始化
        this.readWriteCacheMap =
                /**
                 * 初始读写缓存容量大小, 默认1000
                 * @see DefaultEurekaServerConfig#getInitialCapacityOfResponseCache()
                 */
                CacheBuilder.newBuilder().initialCapacity(serverConfig.getInitialCapacityOfResponseCache())
                        // 读写缓存默认过期时间 180s
                        .expireAfterWrite(serverConfig.getResponseCacheAutoExpirationInSeconds(), TimeUnit.SECONDS)
                        // 移除的监听器
                        .removalListener(new RemovalListener<Key, Value>() {
                            @Override
                            public void onRemoval(RemovalNotification<Key, Value> notification) {
                                Key removedKey = notification.getKey();
                                if (removedKey.hasRegions()) {
                                    Key cloneWithNoRegions = removedKey.cloneWithoutRegions();
                                    regionSpecificKeys.remove(cloneWithNoRegions, removedKey);
                                }
                            }
                        })
                        // 加载
                        .build(new CacheLoader<Key, Value>() {
                            @Override
                            public Value load(Key key) throws Exception {
                                if (key.hasRegions()) {
                                    Key cloneWithNoRegions = key.cloneWithoutRegions();
                                    regionSpecificKeys.put(cloneWithNoRegions, key);
                                }
                                // 服务发现: 读写缓存去真实数据获取数据
                                Value value = generatePayload(key);
                                return value;
                            }
                        });

        // 只读缓存初始化: 通过定时器每30s从读写缓存中拿取
        if (shouldUseReadOnlyResponseCache) {
            timer.schedule(getCacheUpdateTask(),
                    new Date(((System.currentTimeMillis() / responseCacheUpdateIntervalMs) * responseCacheUpdateIntervalMs)
                            + responseCacheUpdateIntervalMs),
                    responseCacheUpdateIntervalMs);
        }

        try {
            Monitors.registerObject(this);
        } catch (Throwable e) {
            logger.warn("Cannot register the JMX monitor for the InstanceRegistry", e);
        }
    }

    // 定时任务: 只读缓存从读写缓存中拿
    private TimerTask getCacheUpdateTask() {
        return new TimerTask() {
            @Override
            public void run() {
                logger.debug("Updating the client cache from response cache");
                // todo 只更新 只读缓存 现有的?,
                // 只读缓存的新增在服务发现时, 就已经加入到读写缓存中了
                for (Key key : readOnlyCacheMap.keySet()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Updating the client cache from response cache for key : {} {} {} {}",
                                key.getEntityType(), key.getName(), key.getVersion(), key.getType());
                    }
                    try {
                        CurrentRequestVersion.set(key.getVersion());
                        // 从读写缓存中获取
                        Value cacheValue = readWriteCacheMap.get(key);
                        // 从只读缓存中获取
                        Value currentCacheValue = readOnlyCacheMap.get(key);
                        // 如果读写缓存和只读缓存的值不一致, 则将只读缓存中的值更新为读写缓存的值
                        if (cacheValue != currentCacheValue) {
                            readOnlyCacheMap.put(key, cacheValue);
                        }
                    } catch (Throwable th) {
                        logger.error("Error while updating the client cache from response cache for key {}", key.toStringCompact(), th);
                    } finally {
                        CurrentRequestVersion.remove();
                    }
                }
            }
        };
    }

    /**
     * Get the cached information about applications.
     *
     * <p>
     * If the cached information is not available it is generated on the first
     * request. After the first request, the information is then updated
     * periodically by a background thread.
     * </p>
     *
     * @param key the key for which the cached information needs to be obtained.
     * @return payload which contains information about the applications.
     */
    public String get(final Key key) {
        return get(key, shouldUseReadOnlyResponseCache);
    }

    @VisibleForTesting
    String get(final Key key, boolean useReadOnlyCache) {
        Value payload = getValue(key, useReadOnlyCache);
        if (payload == null || payload.getPayload().equals(EMPTY_PAYLOAD)) {
            return null;
        } else {
            return payload.getPayload();
        }
    }

    /**
     * 缓存
     * Get the compressed information about the applications.
     *
     * @param key
     *            the key for which the compressed cached information needs to
     *            be obtained.
     * @return compressed payload which contains information about the
     *         applications.
     */
    public byte[] getGZIP(Key key) {
        // 拿到微服务信息
        Value payload = getValue(key, shouldUseReadOnlyResponseCache);
        if (payload == null) {
            return null;
        }
        return payload.getGzipped();
    }

    @Override
    public void stop() {
        timer.cancel();
        Monitors.unregisterObject(this);
    }

    /**
     * 失效读写缓存: 使特定应用程序的缓存无效。
     * Invalidate the cache of a particular application.
     *
     * @param appName the application name of the application. 微服务名
     */
    @Override
    public void invalidate(String appName, @Nullable String vipAddress, @Nullable String secureVipAddress) {
        for (Key.KeyType type : Key.KeyType.values()) {
            for (Version v : Version.values()) {
                invalidate(
                        // 更新当前微服务名所在的微服务缓存 EurekaAccept.full/compact 表示数据压缩/未压缩
                        new Key(Key.EntityType.Application, appName, type, v, EurekaAccept.full),
                        new Key(Key.EntityType.Application, appName, type, v, EurekaAccept.compact),
                        // 更新全量微服务缓存
                        new Key(Key.EntityType.Application, ALL_APPS, type, v, EurekaAccept.full),
                        new Key(Key.EntityType.Application, ALL_APPS, type, v, EurekaAccept.compact),
                        // 更新增量微服务缓存
                        new Key(Key.EntityType.Application, ALL_APPS_DELTA, type, v, EurekaAccept.full),
                        new Key(Key.EntityType.Application, ALL_APPS_DELTA, type, v, EurekaAccept.compact)
                );
                if (null != vipAddress) {
                    invalidate(new Key(Key.EntityType.VIP, vipAddress, type, v, EurekaAccept.full));
                }
                if (null != secureVipAddress) {
                    invalidate(new Key(Key.EntityType.SVIP, secureVipAddress, type, v, EurekaAccept.full));
                }
            }
        }
    }

    /**
     * 失效读写缓存: 给定键列表，使缓存信息无效。
     * Invalidate the cache information given the list of keys.
     *
     * @param keys the list of keys for which the cache information needs to be invalidated.
     */
    public void invalidate(Key... keys) {
        for (Key key : keys) {
            logger.debug("Invalidating the response cache key : {} {} {} {}, {}",
                    key.getEntityType(), key.getName(), key.getVersion(), key.getType(), key.getEurekaAccept());

            /**
             * 从读写缓存中移除
             * @see LocalCache.LocalManualCache#invalidate(java.lang.Object)
             */
            readWriteCacheMap.invalidate(key);
            Collection<Key> keysWithRegions = regionSpecificKeys.get(key);
            if (null != keysWithRegions && !keysWithRegions.isEmpty()) {
                for (Key keysWithRegion : keysWithRegions) {
                    logger.debug("Invalidating the response cache key : {} {} {} {} {}",
                            key.getEntityType(), key.getName(), key.getVersion(), key.getType(), key.getEurekaAccept());
                    readWriteCacheMap.invalidate(keysWithRegion);
                }
            }
        }
    }

    /**
     * Gets the version number of the cached data.
     *
     * @return teh version number of the cached data.
     */
    @Override
    public AtomicLong getVersionDelta() {
        return versionDelta;
    }

    /**
     * Gets the version number of the cached data with remote regions.
     *
     * @return teh version number of the cached data with remote regions.
     */
    @Override
    public AtomicLong getVersionDeltaWithRegions() {
        return versionDeltaWithRegions;
    }

    /**
     * @deprecated use instance method {@link #getVersionDelta()}
     *
     * Gets the version number of the cached data.
     *
     * @return teh version number of the cached data.
     */
    @Deprecated
    public static AtomicLong getVersionDeltaStatic() {
        return versionDeltaLegacy;
    }

    /**
     * @deprecated use instance method {@link #getVersionDeltaWithRegions()}
     *
     * Gets the version number of the cached data with remote regions.
     *
     * @return teh version number of the cached data with remote regions.
     */
    @Deprecated
    public static AtomicLong getVersionDeltaWithRegionsLegacy() {
        return versionDeltaWithRegionsLegacy;
    }

    /**
     * Get the number of items in the response cache.
     *
     * @return int value representing the number of items in response cache.
     */
    @Monitor(name = "responseCacheSize", type = DataSourceType.GAUGE)
    public int getCurrentSize() {
        return readWriteCacheMap.asMap().size();
    }

    /**
     * 从缓存中获取
     *
     * Get the payload in both compressed and uncompressed form.
     * 以压缩和未压缩的形式获取有效负载。
     * @param useReadOnlyCache 是否使用只读缓存, 默认为true
     */
    @VisibleForTesting
    Value getValue(final Key key, boolean useReadOnlyCache) {
        Value payload = null;
        try {
            // 判断是否开启只读缓存, 默认开启
            if (useReadOnlyCache) {
                // 从只读缓存中拿
                final Value currentPayload = readOnlyCacheMap.get(key);
                if (currentPayload != null) {
                    // 从只读缓存中获取到了
                    payload = currentPayload;
                } else {
                    // 拿不到去读写缓存中拿
                    payload = readWriteCacheMap.get(key);
                    // 将读写缓存中读到的结果同步到只读缓存
                    readOnlyCacheMap.put(key, payload);
                }
            } else {
                // 去读写缓存中拿
                payload = readWriteCacheMap.get(key);
            }
        } catch (Throwable t) {
            logger.error("Cannot get value for key : {}", key, t);
        }
        return payload;
    }

    /**
     * 获取真实数据
     *
     * Generate pay load with both JSON and XML formats for all applications.
     * 为所有应用程序生成JSON和XML格式的pay load。
     */
    private String getPayLoad(Key key, Applications apps) {
        EncoderWrapper encoderWrapper = serverCodecs.getEncoder(key.getType(), key.getEurekaAccept());
        String result;
        try {
            result = encoderWrapper.encode(apps);
        } catch (Exception e) {
            logger.error("Failed to encode the payload for all apps", e);
            return "";
        }
        if(logger.isDebugEnabled()) {
            logger.debug("New application cache entry {} with apps hashcode {}", key.toStringCompact(), apps.getAppsHashCode());
        }
        return result;
    }

    /**
     * Generate pay load with both JSON and XML formats for a given application.
     */
    private String getPayLoad(Key key, Application app) {
        if (app == null) {
            return EMPTY_PAYLOAD;
        }

        EncoderWrapper encoderWrapper = serverCodecs.getEncoder(key.getType(), key.getEurekaAccept());
        try {
            return encoderWrapper.encode(app);
        } catch (Exception e) {
            logger.error("Failed to encode the payload for application {}", app.getName(), e);
            return "";
        }
    }

    /**
     * 服务发现: 读写缓存去真实数据获取数据
     * 为给定的密钥生成有效负载。
     * Generate pay load for the given key.
     */
    private Value generatePayload(Key key) {
        Stopwatch tracer = null;
        try {
            String payload;
            switch (key.getEntityType()) {
                case Application:
                    boolean isRemoteRegionRequested = key.hasRegions();
                    // 全量获取
                    if (ALL_APPS.equals(key.getName())) {
                        /**
                         * 不管是否需要远程区域, 最终都是调用 {@link AbstractInstanceRegistry#getApplicationsFromMultipleRegions(java.lang.String[])} 去真实数据获取全量数据
                         * 在这个方法中 会遍历真实的微服务缓存池 {@link AbstractInstanceRegistry#registry}
                         */
                        // 是否需要远程区域, 一般用不到
                        if (isRemoteRegionRequested) {
                            tracer = serializeAllAppsWithRemoteRegionTimer.start();
                            /**
                             * registry.getApplicationsFromMultipleRegions() 最后调用 {@link AbstractInstanceRegistry#getApplicationsFromMultipleRegions(java.lang.String[])} 去真实数据获取全量数据
                             */
                            payload = getPayLoad(key, registry.getApplicationsFromMultipleRegions(key.getRegions()));
                        } else {
                            tracer = serializeAllAppsTimer.start();
                            /**
                             * registry.getApplications() 最后调用 {@link AbstractInstanceRegistry#getApplicationsFromMultipleRegions(java.lang.String[])} 去真实数据获取全量数据
                             */
                            payload = getPayLoad(key, registry.getApplications());
                        }
                    // 增量获取
                    } else if (ALL_APPS_DELTA.equals(key.getName())) {
                        if (isRemoteRegionRequested) {
                            tracer = serializeDeltaAppsWithRemoteRegionTimer.start();
                            versionDeltaWithRegions.incrementAndGet();
                            versionDeltaWithRegionsLegacy.incrementAndGet();
                            /**
                             * registry.getApplicationDeltasFromMultipleRegions() 最后调用 {@link AbstractInstanceRegistry#getApplicationDeltasFromMultipleRegions(java.lang.String[])} 去真实数据获取增量数据
                             */
                            payload = getPayLoad(key,
                                    registry.getApplicationDeltasFromMultipleRegions(key.getRegions()));
                        } else {
                            tracer = serializeDeltaAppsTimer.start();
                            versionDelta.incrementAndGet();
                            versionDeltaLegacy.incrementAndGet();
                            payload = getPayLoad(key, registry.getApplicationDeltas());
                        }
                    } else {
                        tracer = serializeOneApptimer.start();
                        payload = getPayLoad(key, registry.getApplication(key.getName()));
                    }
                    break;
                case VIP:
                case SVIP:
                    tracer = serializeViptimer.start();
                    payload = getPayLoad(key, getApplicationsForVip(key, registry));
                    break;
                default:
                    logger.error("Unidentified entity type: {} found in the cache key.", key.getEntityType());
                    payload = "";
                    break;
            }
            return new Value(payload);
        } finally {
            if (tracer != null) {
                tracer.stop();
            }
        }
    }

    private static Applications getApplicationsForVip(Key key, AbstractInstanceRegistry registry) {
        logger.debug(
                "Retrieving applications from registry for key : {} {} {} {}",
                key.getEntityType(), key.getName(), key.getVersion(), key.getType());
        Applications toReturn = new Applications();
        Applications applications = registry.getApplications();
        for (Application application : applications.getRegisteredApplications()) {
            Application appToAdd = null;
            for (InstanceInfo instanceInfo : application.getInstances()) {
                String vipAddress;
                if (Key.EntityType.VIP.equals(key.getEntityType())) {
                    vipAddress = instanceInfo.getVIPAddress();
                } else if (Key.EntityType.SVIP.equals(key.getEntityType())) {
                    vipAddress = instanceInfo.getSecureVipAddress();
                } else {
                    // should not happen, but just in case.
                    continue;
                }

                if (null != vipAddress) {
                    String[] vipAddresses = vipAddress.split(",");
                    Arrays.sort(vipAddresses);
                    if (Arrays.binarySearch(vipAddresses, key.getName()) >= 0) {
                        if (null == appToAdd) {
                            appToAdd = new Application(application.getName());
                            toReturn.addApplication(appToAdd);
                        }
                        appToAdd.addInstance(instanceInfo);
                    }
                }
            }
        }
        toReturn.setAppsHashCode(toReturn.getReconcileHashCode());
        logger.debug(
                "Retrieved applications from registry for key : {} {} {} {}, reconcile hashcode: {}",
                key.getEntityType(), key.getName(), key.getVersion(), key.getType(),
                toReturn.getReconcileHashCode());
        return toReturn;
    }

    /**
     * The class that stores payload in both compressed and uncompressed form.
     *
     */
    public class Value {
        private final String payload;
        private byte[] gzipped;

        public Value(String payload) {
            this.payload = payload;
            if (!EMPTY_PAYLOAD.equals(payload)) {
                Stopwatch tracer = compressPayloadTimer.start();
                try {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    GZIPOutputStream out = new GZIPOutputStream(bos);
                    byte[] rawBytes = payload.getBytes();
                    out.write(rawBytes);
                    // Finish creation of gzip file
                    out.finish();
                    out.close();
                    bos.close();
                    gzipped = bos.toByteArray();
                } catch (IOException e) {
                    gzipped = null;
                } finally {
                    if (tracer != null) {
                        tracer.stop();
                    }
                }
            } else {
                gzipped = null;
            }
        }

        public String getPayload() {
            return payload;
        }

        public byte[] getGzipped() {
            return gzipped;
        }

    }

}
