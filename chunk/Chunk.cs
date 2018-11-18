namespace FileCompression
{
    class Chunk
    {
        public bool IsStopper { get; set; }
        public byte[] Content { get; set; }
        public long InitialOffset { get; set; }
        public int InitialSize { get; set; }
    }
}