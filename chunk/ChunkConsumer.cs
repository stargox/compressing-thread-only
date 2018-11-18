using System;
using System.Threading;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;

namespace FileCompression
{
    class ChunkConsumer : IDisposable
    {
        private readonly object _locker = new object();
        private FileInfo _fileInfo;
        private bool _isWorkerStarted = false;
        private bool _isWorkerCancelled = false;
        private Exception _workerException;
        private Thread _workerThread;
        private Queue<Chunk> _chunkQueue;
        private readonly int _maxQueueSize = 100;
        Action<BinaryWriter, Chunk> _chunkSetter;

        public ChunkConsumer(FileInfo fileInfo, Action<BinaryWriter, Chunk> chunkSetter) {
            _fileInfo = fileInfo;
            _chunkSetter = chunkSetter;
            _chunkQueue = new Queue<Chunk>();
             _workerThread = new Thread(StartWritingChunks);
        }

        public void SetChunk(Chunk chunk) {
            if (!_isWorkerStarted) RunWorker();

            lock (_locker) {
                while (_chunkQueue.Count > _maxQueueSize && !_isWorkerCancelled) Monitor.Wait(_locker);
                
                if (_workerException != null) throw _workerException;
                if (_isWorkerCancelled) return;
            }

            lock (_locker) {
                _chunkQueue.Enqueue(chunk);

                Monitor.Pulse(_locker);
            }
        }

        public void WaitForFinish() {
            lock (_locker) {
                while (_chunkQueue.Count > 0 && !_isWorkerCancelled) Monitor.Wait(_locker);

                if (_workerException != null) throw _workerException;
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

        private void StartWritingChunks() {
            try {
                using (var fileStream = _fileInfo.OpenWrite())
                using (var writer = new BinaryWriter(fileStream)) {
                    while(true) {
                        Chunk chunk;
                        
                        lock (_locker) {
                            while (_chunkQueue.Count == 0 && !_isWorkerCancelled) Monitor.Wait(_locker);

                            if (_isWorkerCancelled) return;
                        }

                        lock (_locker) {
                            chunk = _chunkQueue.Dequeue();
                            
                            Monitor.Pulse(_locker);
                        }

                        if (chunk.IsStopper) return;

                        _chunkSetter(writer, chunk);
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