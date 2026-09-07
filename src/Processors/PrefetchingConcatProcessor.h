#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <deque>
#include <vector>


namespace DB
{

/** Like ConcatProcessor: has N inputs and one output, all with the same structure.
  * Outputs data from inputs in order (first all data from input 0, then input 1, etc.).
  *
  * Unlike ConcatProcessor, it keeps every not-yet-consumed input needed instead of only the
  * current one, and buffers what they produce, so the pipeline executor can run all of their
  * sources in parallel while we are still emitting the current input.
  *
  * The read-ahead is bounded by a row/byte budget shared by all inputs, not by a fixed number
  * of inputs or chunks. That budget is the one a single merge input would have got from
  * `BufferChunksTransform`, which is exactly what this processor replaces for the streams of
  * one part: the total read-ahead becomes proportional to the number of parts instead of the
  * number of streams, which is the memory this saves, while nothing caps how many of the
  * part's streams may read at the same time - capping that is what would serialize reading
  * and cost throughput when the sources are decompression- or latency-bound.
  * Once the budget is spent, a produced chunk stays on its input port and stalls its source,
  * the same backpressure a full `BufferChunksTransform` applies to a merge input.
  * As in `BufferChunksTransform`, the two limits are combined with OR.
  *
  * This is useful for read-in-order optimization where a single part is split
  * into non-overlapping range groups read by separate sources: we want parallel
  * IO/filtering across groups while preserving the sorted order via concatenation.
  */
class PrefetchingConcatProcessor final : public IProcessor
{
public:
    PrefetchingConcatProcessor(SharedHeader header, size_t num_inputs, size_t max_rows_to_buffer_, size_t max_bytes_to_buffer_);

    String getName() const override { return "PrefetchingConcat"; }

    Status prepare() override;

    OutputPort & getOutputPort() { return outputs.front(); }

private:
    bool hasBufferRoom() const { return num_buffered_rows < max_rows_to_buffer || num_buffered_bytes < max_bytes_to_buffer; }

    size_t current_input_idx = 0;
    size_t max_rows_to_buffer;
    size_t max_bytes_to_buffer;
    size_t num_buffered_rows = 0;
    size_t num_buffered_bytes = 0;
    std::vector<std::deque<Chunk>> buffers;
};

}
