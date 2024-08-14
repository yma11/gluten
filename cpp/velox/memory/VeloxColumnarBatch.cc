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
#include "VeloxColumnarBatch.h"
#include "compute/VeloxRuntime.h"
#include "utils/Timer.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace gluten {

using namespace facebook;
using namespace facebook::velox;

namespace {

RowVectorPtr makeRowVector(
    std::vector<std::string> childNames,
    const std::vector<VectorPtr>& children,
    int32_t numRows,
    velox::memory::MemoryPool* pool) {
  std::vector<std::shared_ptr<const Type>> childTypes;
  childTypes.resize(children.size());
  for (int i = 0; i < children.size(); i++) {
    childTypes[i] = children[i]->type();
  }
  auto rowType = ROW(std::move(childNames), std::move(childTypes));
  return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr), numRows, std::move(children));
}
} // namespace

void VeloxColumnarBatch::ensureFlattened() {
  if (flattened_) {
    return;
  }
  ScopedTimer timer(&exportNanos_);
  for (auto& child : rowVector_->children()) {
    facebook::velox::BaseVector::flattenVector(child);
    if (child->isLazy()) {
      child = child->as<facebook::velox::LazyVector>()->loadedVectorShared();
      VELOX_DCHECK_NOT_NULL(child);
    }
    // In case of output from Limit, RowVector size can be smaller than its children size.
    if (child->size() > rowVector_->size()) {
      child = child->slice(0, rowVector_->size());
    }
  }
  flattened_ = true;
}

std::shared_ptr<ArrowSchema> VeloxColumnarBatch::exportArrowSchema() {
  auto out = std::make_shared<ArrowSchema>();
  ensureFlattened();
  velox::exportToArrow(rowVector_, *out, ArrowUtils::getBridgeOptions());
  return out;
}

std::shared_ptr<ArrowArray> VeloxColumnarBatch::exportArrowArray() {
  auto out = std::make_shared<ArrowArray>();
  ensureFlattened();
  velox::exportToArrow(rowVector_, *out, rowVector_->pool(), ArrowUtils::getBridgeOptions());
  return out;
}

int64_t VeloxColumnarBatch::numBytes() {
  ensureFlattened();
  return rowVector_->estimateFlatSize();
}

velox::RowVectorPtr VeloxColumnarBatch::getRowVector() const {
  return rowVector_;
}

velox::RowVectorPtr VeloxColumnarBatch::getFlattenedRowVector() {
  ensureFlattened();
  return rowVector_;
}

std::shared_ptr<VeloxColumnarBatch> VeloxColumnarBatch::from(
    facebook::velox::memory::MemoryPool* pool,
    std::shared_ptr<ColumnarBatch> cb) {
  if (cb->getType() == "velox") {
    return std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  }
  if (cb->getType() == "composite") {
    auto composite = std::dynamic_pointer_cast<gluten::CompositeColumnarBatch>(cb);
    auto children = composite->getBatches();
    std::vector<std::string> childNames;
    std::vector<VectorPtr> childVectors;

    for (const auto& child : children) {
      auto asVelox = from(pool, child);
      auto names = facebook::velox::asRowType(asVelox->getRowVector()->type())->names();
      for (const auto& name : names) {
        childNames.push_back(name);
      }
      auto vectors = asVelox->getRowVector()->children();
      for (const auto& vector : vectors) {
        childVectors.push_back(vector);
      }
    }

    auto compositeVeloxVector = makeRowVector(childNames, childVectors, cb->numRows(), pool);
    return std::make_shared<VeloxColumnarBatch>(compositeVeloxVector);
  }
  if (cb->getType() == "composite_reorder") {
    // Initialize children as batch2 children, then insert specified columns in batch1
    auto composite = std::dynamic_pointer_cast<gluten::CompositeReorderColumnarBatch>(cb);
    auto vector1 = from(pool, composite->getBatch1())->getRowVector();
    auto vector2 = from(pool, composite->getBatch2())->getRowVector();
    auto& vector1Names = facebook::velox::asRowType(vector1->type())->names();
    auto& vector2Names = facebook::velox::asRowType(vector2->type())->names();
    std::vector<std::string> childNames;
    std::vector<VectorPtr> childVectors;
    childNames.reserve(composite->numColumns());
    childVectors.reserve(composite->numColumns());
    auto& batch1Indices = composite->getBatch1ColumnIndices();
    for (int32_t i = 0, cb1Index = 0, cb2Index = 0; i < composite->numColumns(); i++) {
      if (batch1Indices[cb1Index] == i) {
        childNames.push_back(vector1Names[cb1Index]);
        childVectors.push_back(vector1->childAt(cb1Index++));
      } else {
        childNames.push_back(vector2Names[cb2Index]);
        childVectors.push_back(vector2->childAt(cb2Index++));
      }
    }
    auto compositeVeloxVector = makeRowVector(std::move(childNames), std::move(childVectors), cb->numRows(), pool);
    return std::make_shared<VeloxColumnarBatch>(compositeVeloxVector);
  }
  auto vp = velox::importFromArrowAsOwner(*cb->exportArrowSchema(), *cb->exportArrowArray(), pool);
  return std::make_shared<VeloxColumnarBatch>(std::dynamic_pointer_cast<velox::RowVector>(vp));
}

std::shared_ptr<ColumnarBatch> VeloxColumnarBatch::select(
    facebook::velox::memory::MemoryPool* pool,
    std::vector<int32_t> columnIndices) {
  std::vector<std::string> childNames;
  std::vector<VectorPtr> childVectors;
  childNames.reserve(columnIndices.size());
  childVectors.reserve(columnIndices.size());
  auto vector = getFlattenedRowVector();
  auto type = facebook::velox::asRowType(vector->type());

  for (uint32_t i = 0; i < columnIndices.size(); i++) {
    auto index = columnIndices[i];
    auto child = vector->childAt(index);
    childNames.push_back(type->nameOf(index));
    childVectors.push_back(child);
  }

  auto rowVector = makeRowVector(std::move(childNames), std::move(childVectors), numRows(), pool);
  return std::make_shared<VeloxColumnarBatch>(rowVector);
}

std::vector<char> VeloxColumnarBatch::toUnsafeRow(int32_t rowId) const {
  auto fast = std::make_unique<facebook::velox::row::UnsafeRowFast>(rowVector_);
  auto size = fast->rowSize(rowId);
  std::vector<char> bytes(size);
  std::memset(bytes.data(), 0, bytes.size());
  fast->serialize(0, bytes.data());
  return bytes;
}

} // namespace gluten
