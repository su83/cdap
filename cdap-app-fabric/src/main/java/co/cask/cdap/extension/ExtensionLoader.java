/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.extension;

import co.cask.cdap.common.utils.DirUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A class to maintain a cache of extensions available in a configured extensions directory. It uses the Java
 * {@link ServiceLoader} architecture to load extensions from the specified directory. It first tries to load
 * extensions using a {@link URLClassLoader} consisting of all jars in the configured extensions directory. For unit
 * tests, it loads extensions using the system classloader.
 *
 * The supported directory structure is:
 * <pre>
 *   [extensions-directory]/[extensions-module-directory]/
 *   [extensions-directory]/[extensions-module-directory]/file-containing-extensions-1.jar
 *   [extensions-directory]/[extensions-module-directory]/file-containing-extensions-2.jar
 *   ...
 *   [extensions-directory]/[extensions-module-directory]/file-containing-extensions-n.jar
 * </pre>
 *
 * Each extensions jar file above can contain multiple extensions.
 * 
 * @param <CACHE_KEY> the key type stored in the cache
 * @param <CACHE_VALUE> the value type stored in the cache
 */
public class ExtensionLoader<CACHE_KEY, CACHE_VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(ExtensionLoader.class);

  private final List<String> extDirs;
  private final Class<CACHE_VALUE> cacheValueClass;
  // The ServiceLoader that loads extension implementation from the CDAP system classloader.
  private final ServiceLoader<CACHE_VALUE> systemExtensionLoader;
  private final Function<CACHE_VALUE, Set<CACHE_KEY>> extensionToKeys;
  private final LoadingCache<CACHE_KEY, CACHE_VALUE> extensionsCache;
  private final CACHE_VALUE defaultExtension;

  public ExtensionLoader(String extDirs, Function<CACHE_VALUE, Set<CACHE_KEY>> extensionToKeys,
                         Class<CACHE_VALUE> cacheValueClass, CACHE_VALUE defaultExtension) {
    this.extDirs = ImmutableList.copyOf(Splitter.on(';').omitEmptyStrings().trimResults().split(extDirs));
    this.cacheValueClass = cacheValueClass;
    this.systemExtensionLoader = ServiceLoader.load(cacheValueClass);
    this.extensionToKeys = extensionToKeys;
    this.extensionsCache = createExtensionsCache();
    this.defaultExtension = defaultExtension;
  }

  /**
   * Returns the extension for the specified key.
   */
  public CACHE_VALUE getExtension(CACHE_KEY key) {
    return extensionsCache.getUnchecked(key);
  }

  /**
   * Returns all cached extensions.
   */
  public Map<CACHE_KEY, CACHE_VALUE> listExtensions() {
    return extensionsCache.asMap();
  }

  /**
   * Preloads the extension cache with all the extensions from the specified extensions directory.
   */
  public void preload() {
    extensionsCache.putAll(findExtensions(null));
  }

  @VisibleForTesting
  void invalidate() {
    extensionsCache.invalidateAll();
  }

  private LoadingCache<CACHE_KEY, CACHE_VALUE> createExtensionsCache() {
    return CacheBuilder.newBuilder().build(new CacheLoader<CACHE_KEY, CACHE_VALUE>() {
      @Override
      public CACHE_VALUE load(CACHE_KEY key) throws Exception {
        Map<CACHE_KEY, CACHE_VALUE> extensions = findExtensions(key);
        // If there is none found in the ext dir, try to look it up from the CDAP system class ClassLoader.
        // This is for the unit-test case, where extensions are part of the test dependency, hence in the
        // unit-test ClassLoader.
        // If no provider was found, returns the NOT_SUPPORTED_PROVIDER so that we won't search again for
        // this program type.
        // Cannot use null because LoadingCache doesn't allow null value
        return Objects.firstNonNull(extensions.get(key), defaultExtension);
      }
    });
  }

  /**
   * Creates a cache for caching extension directory to {@link ServiceLoader} of {@link CACHE_VALUE}.
   */
  private LoadingCache<File, ServiceLoader<CACHE_VALUE>> createServiceLoaderCache() {
    return CacheBuilder.newBuilder().build(new CacheLoader<File, ServiceLoader<CACHE_VALUE>>() {
      @Override
      public ServiceLoader<CACHE_VALUE> load(File dir) throws Exception {
        return createServiceLoader(dir);
      }
    });
  }

  /**
   * Creates a {@link ServiceLoader} from the {@link ClassLoader} created by all jar files under the given directory.
   */
  private ServiceLoader<CACHE_VALUE> createServiceLoader(File dir) {
    List<File> files = new ArrayList<>(DirUtils.listFiles(dir, "jar"));
    Collections.sort(files);

    URL[] urls = Iterables.toArray(Iterables.transform(files, new Function<File, URL>() {
      @Override
      public URL apply(File input) {
        try {
          return input.toURI().toURL();
        } catch (MalformedURLException e) {
          // Shouldn't happen
          throw Throwables.propagate(e);
        }
      }
    }), URL.class);

    URLClassLoader classLoader = new URLClassLoader(urls, ExtensionLoader.class.getClassLoader());
    return ServiceLoader.load(cacheValueClass, classLoader);
  }

  /**
   * Finds the first extension from the given {@link ServiceLoader} that supports the specified key. If the key
   * specified is {@code null}, returns all extensions from the extensions directory.
   */
  private Map<CACHE_KEY, CACHE_VALUE> findExtensions(@Nullable CACHE_KEY key) {
    Map<CACHE_KEY, CACHE_VALUE> extensions = new HashMap<>();
    // A LoadingCache from extension directory to ServiceLoader
    final LoadingCache<File, ServiceLoader<CACHE_VALUE>> serviceLoaderCache = createServiceLoaderCache();
    for (String dir : extDirs) {
      File extDir = new File(dir);
      if (!extDir.isDirectory()) {
        continue;
      }

      // Each module would be under a directory of the extension directory
      for (File moduleDir : DirUtils.listFiles(extDir)) {
        if (!moduleDir.isDirectory()) {
          continue;
        }
        // Try to find a provider that can support the given program type.
        try {
          extensions.putAll(findExtensions(serviceLoaderCache.getUnchecked(moduleDir), key));
        } catch (Exception e) {
          LOG.warn("Exception raised when loading an extension from {}. Extension ignored.", moduleDir, e);
        }
      }
    }
    // For unit tests, try to load the extensions from the system classloader. Only add those extensions which
    // were not already loaded using the extension classloader.
    for (Map.Entry<CACHE_KEY, CACHE_VALUE> entry : findExtensions(systemExtensionLoader, key).entrySet()) {
      if (!extensions.containsKey(entry.getKey())) {
        extensions.put(entry.getKey(), entry.getValue());
      }
    }
    return extensions;
  }

  /**
   * Returns the first available extension that support the specified key using the specified {@link ServiceLoader}.
   * If the key is {@code null}, returns all the extensions in the extensions directory
   */
  private Map<CACHE_KEY, CACHE_VALUE> findExtensions(ServiceLoader<CACHE_VALUE> serviceLoader, CACHE_KEY key) {
    Map<CACHE_KEY, CACHE_VALUE> extensions = new HashMap<>();
    for (CACHE_VALUE provider : serviceLoader) {
      Set<CACHE_KEY> cacheKeys = extensionToKeys.apply(provider);
      if (cacheKeys == null) {
        continue;
      }
      // If key is not specified, add all the supported keys of the provider
      if (key == null) {
        for (CACHE_KEY cacheKey : cacheKeys) {
          extensions.put(cacheKey, provider);
        }
      } else {
        // If a key is specified, and is found in the supported keys of the provider, return the provider
        if (cacheKeys.contains(key)) {
          extensions.put(key, provider);
          return extensions;
        }
      }
    }
    return extensions;
  }
}
