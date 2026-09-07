#include <Processors/PrefetchingConcatProcessor.h>
#include <Processors/Port.h>

namespace DB
{

PrefetchingConcatProcessor::PrefetchingConcatProcessor(
    SharedHeader header, size_t num_inputs, size_t max_rows_to_buffer_, size_t max_bytes_to_buffer_)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts{header})
    , max_rows_to_buffer(max_rows_to_buffer_)
    , max_bytes_to_buffer(max_bytes_to_buffer_)
    , buffers(num_inputs)
{
}

PrefetchingConcatProcessor::Status PrefetchingConcatProcessor::prepare()
{
    auto & output = outputs.front();

    /// Output finished — close everything.
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    /// Pull available data from inputs into their buffers.
    /// This lets their upstream sources continue producing data in parallel.
    ///
    /// The current input is always drained: its chunks are emitted immediately and never
    /// accumulate beyond a single buffered chunk. A non-current input is drained only while
    /// the shared budget has room; once it is spent, the chunk stays on the port and its
    /// source stops, which is the same backpressure a full `BufferChunksTransform` applies
    /// to the source behind a merge input.
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (input.hasData() && (idx == current_input_idx || hasBufferRoom()))
            {
                auto chunk = input.pull();
                num_buffered_rows += chunk.getNumRows();
                num_buffered_bytes += chunk.bytes();
                buffers[idx].push_back(std::move(chunk));
            }
            ++idx;
        }
    }

    /// Keep every upcoming input needed, so all of the part's sources can read and decompress
    /// at the same time — the concurrency they had when each of them was a merge input of its
    /// own. The buffer budget above and the single chunk a port holds are what bound the
    /// memory here; a window on how many inputs may run would bound it too, but it would also
    /// cap read parallelism at that window no matter how many threads the query was given.
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (idx > current_input_idx && !input.isFinished())
                input.setNeeded();
            ++idx;
        }
    }

    /// Skip finished inputs with empty buffers.
    {
        auto it = inputs.begin();
        std::advance(it, current_input_idx);
        while (it != inputs.end() && it->isFinished() && buffers[current_input_idx].empty())
        {
            ++it;
            ++current_input_idx;
        }
    }

    if (current_input_idx >= buffers.size())
    {
        output.finish();
        return Status::Finished;
    }

    /// Try to output from the current input's buffer.
    if (!buffers[current_input_idx].empty())
    {
        auto chunk = std::move(buffers[current_input_idx].front());
        buffers[current_input_idx].pop_front();

        num_buffered_rows -= chunk.getNumRows();
        num_buffered_bytes -= chunk.bytes();

        output.push(std::move(chunk));
        return Status::PortFull;
    }

    /// Buffer is empty — request data from the current input.
    {
        auto it = inputs.begin();
        std::advance(it, current_input_idx);
        it->setNeeded();
    }

    return Status::NeedData;
}

}
