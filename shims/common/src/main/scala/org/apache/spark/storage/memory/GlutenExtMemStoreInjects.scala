/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.storage.memory

import org.apache.gluten.storage.memory.NativeDataCache

class GlutenExtMemStoreInjects {
  def initExternalCache(size: Long, memoryStore: ExternalMemoryStore): Unit = {
    // scalastyle:off println
    println("*******initializing data cache through injects.")
    // scalastyle:on println
    val reservationListener = new ExtMemStoreReservationListener(memoryStore)
    NativeDataCache.setAsyncDataCache(size, reservationListener)
  }
  def evictEntriesToFreeSpace(spaceToFree: Long): Long = {
    // scalastyle:off println
    println("*******Will shrink data cache with size" + spaceToFree + " bytes.")
    // scalastyle:on println
    val shrinkedSize = NativeDataCache.shrinkAsyncDataCache(spaceToFree)
    // scalastyle:off println
    println("*******Shrinked size: " + shrinkedSize + " bytes.")
    // scalastyle:on println
    shrinkedSize
  }
}
