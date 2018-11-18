using System;
using System.Threading;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;

namespace FileCompression
{
    class FileCompressor
    {
        private static void CompressFile(FileInfo sourceFileInfo, FileInfo destinationFileInfo) {
            short chunkMaxSize = 1024;

            Chunk loadChunk(BinaryReader reader) {
                byte[] chunkContent = new byte[chunkMaxSize];
                long initialOffset = reader.BaseStream.Position;
                var chunkSize = reader.Read(chunkContent, 0, chunkContent.Length);

                if (chunkSize < chunkContent.Length) {
                    byte[] updChunkContent = new byte[chunkSize];
                    Array.Copy(chunkContent, updChunkContent, chunkSize);
                    chunkContent = updChunkContent;
                }

                var chunk = new Chunk {
                    Content = chunkContent,
                    InitialSize = chunkSize,
                    InitialOffset = initialOffset,
                    IsStopper = chunkContent.Length == 0,
                };
                return chunk;
            };

            void saveChunk(BinaryWriter writer, Chunk chunk) {
                var chunkSizeInfo = BitConverter.GetBytes((short)chunk.Content.Length);
                writer.Write(chunkSizeInfo, 0, chunkSizeInfo.Length); // 2 bytes

                var initialSizeInfo = BitConverter.GetBytes((short)chunk.InitialSize);
                writer.Write(initialSizeInfo, 0, initialSizeInfo.Length); // 2 bytes

                // TODO: save only 5 bytes (if file max size is only 32gb) 
                var initialOffsetInfo = BitConverter.GetBytes(chunk.InitialOffset);
                writer.Write(initialOffsetInfo, 0, initialOffsetInfo.Length); // 8 bytes

                // TODO: if compressed chunk size is larger then original chunk size, save original with some flag
                writer.Write(chunk.Content, 0, chunk.Content.Length);
            };

            Chunk compressChunk(Chunk chunk) {
                byte[] compressedChunkContent;
                using (var memoryStream = new MemoryStream()) {
                    using (var compressionStream = new DeflateStream(memoryStream, CompressionMode.Compress)) {
                        compressionStream.Write(chunk.Content, 0, chunk.Content.Length);
                    }
                    compressedChunkContent = memoryStream.ToArray();
                }
                
                var compressedChunk = new Chunk {
                    InitialSize = chunk.InitialSize,
                    InitialOffset = chunk.InitialOffset,
                    Content = compressedChunkContent,
                };
                return compressedChunk;
            }

            ProcessAllChunks(sourceFileInfo, destinationFileInfo, compressChunk, loadChunk, saveChunk);
        }

        private static void DecompressFile(FileInfo sourceFileInfo, FileInfo destinationFileInfo) {

            Chunk loadChunk(BinaryReader reader) {
                byte[] chunkSizeInfo = new byte[2];
                var chunkSizeInfoSize = reader.Read(chunkSizeInfo, 0, chunkSizeInfo.Length);
                var chunkSize = BitConverter.ToInt16(chunkSizeInfo);

                byte[] initialSizeInfo = new byte[2];
                var initialSizeInfoSize = reader.Read(initialSizeInfo, 0, initialSizeInfo.Length);
                var initialSize = BitConverter.ToInt16(initialSizeInfo);

                byte[] initialOffsetInfo = new byte[8];
                var initialOffsetInfoSize = reader.Read(initialOffsetInfo, 0, initialOffsetInfo.Length);
                var initialOffset = BitConverter.ToInt64(initialOffsetInfo);

                byte[] chunkContent = new byte[chunkSize];
                var chunkContentSize = reader.Read(chunkContent, 0, chunkContent.Length);

                if (chunkSize != chunkContentSize) throw new InvalidDataException();

                var chunk = new Chunk {
                    Content = chunkContent,
                    InitialSize = initialSize,
                    InitialOffset = initialOffset,
                    IsStopper = chunkContent.Length == 0,
                };
                return chunk;
            };

            void saveChunk(BinaryWriter writer, Chunk chunk) {
                writer.BaseStream.Position = chunk.InitialOffset;
                writer.Write(chunk.Content, 0, chunk.Content.Length);
            };

            Chunk decompressChunk(Chunk chunk) {
                byte[] decompressedChunkContent = new byte[chunk.InitialSize];
                using (var memoryStream = new MemoryStream(chunk.Content)) {
                    using (var decompressionStream = new DeflateStream(memoryStream, CompressionMode.Decompress)) {
                        decompressionStream.Read(decompressedChunkContent, 0, chunk.InitialSize);
                    }
                }

                var decompressedChunk = new Chunk {
                    InitialSize = chunk.InitialSize,
                    InitialOffset = chunk.InitialOffset,
                    Content = decompressedChunkContent,
                };
                return decompressedChunk;
            }

            ProcessAllChunks(sourceFileInfo, destinationFileInfo, decompressChunk, loadChunk, saveChunk);
        }

        private static void ProcessAllChunks(FileInfo sourceFileInfo, FileInfo destinationFileInfo, Func<Chunk, Chunk> chunkCompressor, Func<BinaryReader, Chunk> chunkGetter, Action<BinaryWriter, Chunk> chunkSetter) {
            var totalBytesCount = sourceFileInfo.Length;
            int degreeOfParallelism = Environment.ProcessorCount;
            long offsetByteIndex = 0;
            var startTime = DateTime.Now;
            var prevProgress = 0d;

            void ShowCurrentProgress (int chunkSize) {
                offsetByteIndex += chunkSize;
                var currProgress = (double)offsetByteIndex / totalBytesCount * 100;

                if (currProgress - prevProgress < .1) return;

                var timeElapsed = DateTime.Now - startTime;
                
                Console.Write($"\rProgress: {currProgress.ToString("F1")}%, Time elapsed: {timeElapsed.Hours}:{timeElapsed.Minutes}:{timeElapsed.Seconds}");

                prevProgress = currProgress;
            }

            try {
                using (var compressor = new ChunkCompressor(chunkCompressor, degreeOfParallelism))
                using(var producer = new ChunkProducer(sourceFileInfo, chunkGetter))
                using(var consumer = new ChunkConsumer(destinationFileInfo, chunkSetter)) {
                    bool compressionFinished = false;
                    
                    do {
                        var now = DateTime.Now;
                        producer.GetChunk(out Chunk chunk);

                        ShowCurrentProgress(chunk.Content.Length);

                        compressor.CompressChunk(chunk, (compressedChunk) => {
                            consumer.SetChunk(compressedChunk);
                            if (compressedChunk.IsStopper) compressionFinished = true;
                        });
                    } while(!compressionFinished);

                    consumer.WaitForFinish();
                }
            }
            catch {
                // cleanup created file
                if (File.Exists(destinationFileInfo.FullName)) destinationFileInfo.Delete();
                throw;
            }

        }

        static void Main(string[] args)
        {
            try {
                if (args.Length != 3) throw new ArgumentException("Invalid arguments. Should be: [compress | decompress] [source name] [result name]");

                var compressionMode = args[0];
                var sourceFileName = args[1];
                var destinationFileName = args[2];

                if (compressionMode != "compress" && compressionMode != "decompress") throw new FileNotFoundException($"Available compression modes: compress | decompress");

                FileInfo sourceFileInfo = new FileInfo(sourceFileName);
                FileInfo destinationFileInfo = new FileInfo(destinationFileName);

                if (!sourceFileInfo.Exists) throw new FileNotFoundException($"The file '{sourceFileInfo.Name}' was not found");
                if (destinationFileInfo.Exists) throw new IOException($"The file with name '{destinationFileInfo.Name}' already exists");

                if (sourceFileInfo.Length == 0) throw new FileNotFoundException($"The file '{sourceFileInfo.Name}' size is zero");

                if (compressionMode == "compress") CompressFile(sourceFileInfo, destinationFileInfo);
                else DecompressFile(sourceFileInfo, destinationFileInfo);
            }
            catch (Exception exception) {
                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error:");
                Console.ResetColor();
                Console.WriteLine(exception.Message);
                Console.WriteLine();
            }
        }
    }
}
