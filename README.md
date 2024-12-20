Model and Parallelism
--model={model_id} 
--tensor-parallel-size=8
--model={model_id}: Specifies the model to be loaded and served.
--tensor-parallel-size=8: Sets the degree of tensor parallelism. This splits the model across multiple GPUs to enable serving of larger models or to improve throughput.
Memory Management
--swap-space=32 
--gpu-memory-utilization=0.95
--swap-space=32: Allocates 32GB of CPU memory as swap space, allowing the server to handle more concurrent requests by offloading some data to CPU memory.
--gpu-memory-utilization=0.95: Sets the target GPU memory utilization to 95%, allowing more efficient use of available GPU memory.
Model Configuration
--max-model-len-8192
--max-model-len-8192: Sets the maximum sequence length that the model can handle to 8192 tokens.
Logging and Caching
--disable-log-stats 
--enable-prefix-caching
--disable-log-stats: Turns off logging of performance statistics, which can slightly improve performance.
--enable-prefix-caching: Enables caching of common prompt prefixes, which can speed up processing of similar requests.
Concurrency and Batching
--max-num-seqs=1000 
--max-num-batched-tokens=8192
--max-num-seqs=1000: Sets the maximum number of sequences that can be processed concurrently to 1000.
--max-num-batched-tokens=8192: Sets the maximum number of tokens that can be processed in a single batch to 8192.
Advanced Features
--dynamic-batching 
--enable-chunked-prefill
--dynamic-batching: Enables dynamic batching, which can improve throughput by grouping similar requests together.
--enable-chunked-prefill: Enables chunked prefill, which can improve memory efficiency for long sequences by processing them in chunks.
