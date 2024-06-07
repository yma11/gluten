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

#pragma once

#include "memory/AllocationListener.h"
#include "velox/common/memory/MmapAllocator.h"

namespace gluten {

class VeloxListenableMmapAllocator final : public facebook::velox::memory::MmapAllocator {
 public:
  VeloxListenableMmapAllocator(
      facebook::velox::memory::MmapAllocator* delegated,
      AllocationListener* listener,
      const Options& options)
      : facebook::velox::memory::MmapAllocator(options) {
    delegated_ = delegated;
    listener_ = listener;
  }

 public:
  int64_t freeNonContiguous(facebook::velox::memory::Allocation& allocation) override;

  bool allocateNonContiguousWithoutRetry(
      const facebook::velox::memory::MemoryAllocator::SizeMix& sizeMix,
      facebook::velox::memory::Allocation& out) override;

 private:
  facebook::velox::memory::MmapAllocator* delegated_;
  AllocationListener* listener_;
};

} // namespace gluten
