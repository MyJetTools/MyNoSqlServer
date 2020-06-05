using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MyNoSqlServer.Common
{
    
    public class ChunkedStream : Stream, IMyMemory
    {

        private readonly List<ReadOnlyMemory<byte>> _items = new List<ReadOnlyMemory<byte>>();
        
        public override void Flush()
        {
            
        }
        
        private int CopyToBuffer(int position, byte[] buffer, int offset, int count)
        {
            var (chunkIndex, chunkOffset) = CalcMemoryPosition(position);

            var totalBytesCopied = 0;


            var dest = buffer.AsSpan(offset);
            
            while (chunkIndex<_items.Count)
            {
                var chunk = _items[chunkIndex].Slice(chunkOffset);

                var bytesToCopyCount = count-totalBytesCopied;

                if (bytesToCopyCount > chunk.Length)
                    bytesToCopyCount = chunk.Length;
                
                
                if (bytesToCopyCount>=chunk.Length)
                    chunk.Span.CopyTo(dest);
                else
                    chunk.Slice(0, bytesToCopyCount).Span.CopyTo(dest);

                totalBytesCopied += bytesToCopyCount;
                
                if (totalBytesCopied>=count)
                    break;
                
                dest = buffer.AsSpan(totalBytesCopied);
                
                chunkIndex++;
                chunkOffset = 0;

            }


            Position += totalBytesCopied;
            return totalBytesCopied;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var len = (int)(_length - Position);

            if (len == 0)
                return 0;
            
            if (count > len)
                count = len;
            
            return CopyToBuffer((int) Position, buffer, offset, count);
        }

        
        private byte[] _cachedData;
        
        public byte[] AsArray()
        {
            if (_cachedData != null)
                return _cachedData;

            var len = _items.Sum(itm => itm.Length);

            var result = new byte[len];
            var i = 0;

            foreach (var item in _items)
                for (var j = 0; j < item.Length; j++)
                {
                    result[i] = item.Span[j];
                    i++;
                }

            _cachedData = result;

            return _cachedData;
        }

        public IEnumerable<(byte item, int index)> Enumerate()
        {
            var i = 0;
            foreach (var item in _items)
                for (var j = 0; j < item.Length; j++)
                {
                    yield return (item.Span[j], i);
                    i++;
                }
        }

        public ReadOnlyMemory<byte> Slice(int startPosition, int len)
        {
            // If we have already cached array - we use it;
            if (_cachedData != null)
                return new ReadOnlyMemory<byte>(AsArray(), startPosition, len);


            // Checking if we Have Full Sequence already;
            var (chunkIndex, chunkOffset) = CalcMemoryPosition(startPosition);
            if (_items[chunkIndex].Length - chunkOffset >= len)
                return _items[chunkIndex].Slice(chunkOffset, len);


            // Create a new cache and return it slice
            return new ReadOnlyMemory<byte>(AsArray(), startPosition, len);
        }

        public ReadOnlySpan<byte> Span => AsArray();

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (count == 0)
                return;

            _cachedData = null;
            
            _items.Add( new ReadOnlyMemory<byte>( buffer, offset, count));
            _length += count;
        }
        
        public void Write(ReadOnlyMemory<byte> arraySpan)
        {
            if (arraySpan.Length == 0)
                return;

            _cachedData = null;
            
            _items.Add(arraySpan);
            _length += arraySpan.Length;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;

        private long _length;
        public override long Length => _length;

        private (int chunkIndex, int offset) CalcMemoryPosition(int position)
        {

            
            for (var chunkIndex = 0; chunkIndex < _items.Count; chunkIndex++)
            {
                if (position < _items[chunkIndex].Length)
                    return (chunkIndex, position);

                position -= _items[chunkIndex].Length;
            }
            
            throw new IndexOutOfRangeException($"Index {position} is out of range of range of the stream");
        }

        public override long Position { get; set; }
        
        public static ChunkedStream Create(ReadOnlyMemory<byte> bytes)
        {
            var result= new ChunkedStream();
            result.Write(bytes);
            return result;
        }
        
        public static ChunkedStream Create(IEnumerable<ReadOnlyMemory<byte>> items)
        {
            var result= new ChunkedStream();
            foreach (var item in items)
                result.Write(item);
            return result;
        }
    }
    
}