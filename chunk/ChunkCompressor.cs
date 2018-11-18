using System;
using System.Threading;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Linq;

namespace FileCompression
{
    class ChunkCompressor : IDisposable
    {
        private readonly object _locker = new object();
        private bool _isWorkersStarted = false;
        private bool _isWorkersCancelled = false;
        private Exception _workersException;
        private List<CompressorWorker> _workersPool;
        Func<Chunk, Chunk> _chunkCompressor;

        public ChunkCompressor(Func<Chunk, Chunk> chunkCompressor, int degreeOfParallelism) {
            _chunkCompressor = chunkCompressor;
            _workersPool = new List<CompressorWorker>();

            for (var idx = 0; idx < degreeOfParallelism; idx++) {
                var bufferedThread = new CompressorWorker { Thread = new Thread(startCompressingChunks) };
                _workersPool.Add(bufferedThread);
            }
        }

        public void CompressChunk(Chunk chunk, Action<Chunk> setCompressedChunk) {
            if (!_isWorkersStarted) RunWorkers();

            int stoppedWorkersCount = 0;
            bool chunkPassedToWorker = false;
            do {
                // waiting for any available thread
                int availableThreadIdx = -1;
                int finishedThreadIdx = -1;
                lock (_locker) {
                    while (!_workersPool.Any(t => t.State != CompressorWorkerState.Working) && !_isWorkersCancelled) Monitor.Wait(_locker);

                    if (_workersException != null) throw _workersException;
                    if (_isWorkersCancelled) return;

                    availableThreadIdx = _workersPool.FindIndex(t => t.State == CompressorWorkerState.Available);
                    finishedThreadIdx = availableThreadIdx == -1 ? _workersPool.FindIndex(t => t.State == CompressorWorkerState.Finished) : -1;
                    stoppedWorkersCount = finishedThreadIdx == -1 ? _workersPool.Aggregate<CompressorWorker, int>(0, (acc, curr) => curr.State == CompressorWorkerState.Stopped ? acc + 1: acc) : 0;
                }
                // pass chunk to worker
                if (availableThreadIdx >= 0) {
                    lock (_locker)
                    {
                        var availableWorker = _workersPool[availableThreadIdx];
                        availableWorker.State = CompressorWorkerState.Working;
                        availableWorker.Chunk = chunk;

                        Monitor.Pulse(_locker);
                    }
                    chunkPassedToWorker = true;
                }
                // pass chunk to consumer
                else if (finishedThreadIdx >= 0) {
                    Chunk compressedChunk = null;
                    lock (_locker)
                    {
                        var finishedWorker = _workersPool[finishedThreadIdx];
                        finishedWorker.State = CompressorWorkerState.Available;
                        compressedChunk = finishedWorker.Chunk;

                        Monitor.Pulse(_locker);
                    }
                    setCompressedChunk(compressedChunk);
                }
            } while (chunk.IsStopper && stoppedWorkersCount < _workersPool.Count || !chunkPassedToWorker);

            if (stoppedWorkersCount ==  _workersPool.Count) setCompressedChunk(new Chunk { IsStopper = true });
        }

        private void StopWorkers() {
            lock (_locker) {
                _isWorkersCancelled = true;
                
                Monitor.PulseAll(_locker);
            }
        }

        private void RunWorkers() {
            _isWorkersStarted = true;

            lock(_locker) {
                for (var idx = 0; idx < _workersPool.Count; idx++) {
                    _workersPool[idx].Thread.Start(idx);
                }
            }
        }

        private void startCompressingChunks(object threadIndexObject) {
            int threadIndex = (int)threadIndexObject;

            try {
                bool isCancelled = false;

                while(!isCancelled) {
                    Chunk chunk;
                    
                    lock (_locker) {
                        while ((_workersPool[threadIndex].State != CompressorWorkerState.Working) && !_isWorkersCancelled) Monitor.Wait(_locker);

                        if (_isWorkersCancelled) isCancelled = _isWorkersCancelled;
                        chunk = _workersPool[threadIndex].Chunk;
                    }

                    if (chunk.IsStopper || isCancelled) {
                        lock (_locker)
                        {
                            _workersPool[threadIndex].State = CompressorWorkerState.Stopped;

                            Monitor.Pulse(_locker);
                        }
                        return;
                    }

                    var compressedChunk = _chunkCompressor(chunk);

                    lock (_locker)
                    {
                        _workersPool[threadIndex].State = CompressorWorkerState.Finished;
                        _workersPool[threadIndex].Chunk = compressedChunk;

                        Monitor.Pulse(_locker);
                    }
                }
            }
            catch (Exception exception) {
                lock (_locker) {
                    _isWorkersCancelled = true;
                    _workersException = exception;
                    _workersPool[threadIndex].State = CompressorWorkerState.Stopped;

                    Monitor.PulseAll(_locker);
                }
            }
        }

        public void Dispose() {
            StopWorkers();
        }
    }

    enum CompressorWorkerState {
        Available = 0,
        Working,
        Finished,
        Stopped,
    }

    class CompressorWorker
    {
        public Thread Thread { get; set; }
        public Chunk Chunk { get; set; }
        public CompressorWorkerState State { get; set; }
    }
}