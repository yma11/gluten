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
#include "VeloxListenableMmapAllocator.h"
#include <iostream>

namespace gluten {

int64_t VeloxListenableMmapAllocator::freeNonContiguous(facebook::velox::memory::Allocation& allocation) {
  std::cout << "calling freeNonContiguous in VeloxListenableMmapAllocator" << std::endl;
  int64_t freedSize = delegated_->freeNonContiguous(allocation);
  std::cout << "freedSize is " << freedSize << std::endl;
  listener_->allocationChanged(-freedSize);
  return freedSize;
}

bool VeloxListenableMmapAllocator::allocateNonContiguousWithoutRetry(
    const facebook::velox::memory::MemoryAllocator::SizeMix& sizeMix,
    facebook::velox::memory::Allocation& out) {
  std::cout << "calling allocateNonContiguousWithoutRetry in VeloxListenableMmapAllocator" << std::endl;
  // Firstly assure such amount of memory can be allocated from StorageMemoryPool
  const int64_t acquired = facebook::velox::memory::AllocationTraits::pageBytes(sizeMix.totalPages);
  listener_->allocationChanged(acquired);
  // If comes to here, means allocation succeed as no exception pops
  const bool success = facebook::velox::memory::MmapAllocator::allocateNonContiguousWithoutRetry(sizeMix, out);
  if (success) {
    VELOX_CHECK(!out.empty());
  }
  // Do nothing if allocateNonContiguousWithoutRetry fails as cleanupAllocAndReleaseReservation
  // will be called in MemoryAllocator to release corresponding allocated bytes.
  return success;
}

} // namespace gluten
