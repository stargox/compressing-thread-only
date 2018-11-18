using System;
using System.Threading;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;

namespace FileCompression
{
    class ChunkProducer : IDisposable
    {
        private readonly object _locker = new object();
        private FileInfo _fileInfo;
        private bool _isWorkerStarted = false;
        private bool _isWorkerCancelled = false;
        private Exception _workerException;
        private Thread _workerThread;
        private Queue<Chunk> _chunkQueue;
        private readonly int _maxQueueSize = 10;
        Func<BinaryReader, Chunk> _chunkGetter;

        public ChunkProducer(FileInfo fileInfo, Func<BinaryReader, Chunk> chunkGetter) {
            _fileInfo = fileInfo;
            _chunkGetter = chunkGetter;
            _workerThread = new Thread(startReadingChunks);
            _chunkQueue = new Queue<Chunk>();
        }

        public void GetChunk(out Chunk chunk) {
            if (!_isWorkerStarted) RunWorker();
            chunk = null;

            lock (_locker) {
                while (_chunkQueue.Count == 0 && !_isWorkerCancelled) Monitor.Wait(_locker);

                if (_workerException != null) throw _workerException;
                if (_isWorkerCancelled) return;
            }

            lock (_locker) {
                chunk = _chunkQueue.Dequeue();

                Monitor.Pulse(_locker);
            }
        }

        private void StopWorker() {
            lock (_locker) {
                _isWorkerCancelled = true;

                Monitor.PulseAll(_locker);
            }
        }

        private void RunWorker() {
            _isWorkerStarted = true;
            _workerThread.Start();
        }

        private void startReadingChunks() {
            try {
                using (var fileStream = _fileInfo.OpenRead())
                using (var reader = new BinaryReader(fileStream)){
                    while(true) {
                        lock (_locker) {
                            while (_chunkQueue.Count >= _maxQueueSize && !_isWorkerCancelled) Monitor.Wait(_locker);

                            if (_isWorkerCancelled) return;
                        }

                        var chunk = _chunkGetter(reader);

                        lock (_locker)
                        {
                            _chunkQueue.Enqueue(chunk);

                            Monitor.Pulse(_locker);
                        }

                        if (chunk.IsStopper) return;
                    }
                }
            }
            catch (Exception exception) {
                lock (_locker) {
                    _workerException = exception;
                    _isWorkerCancelled = true;
                    
                    Monitor.PulseAll(_locker);
                }
            }
        }

        public void Dispose() {
            StopWorker();
        }
    }

}