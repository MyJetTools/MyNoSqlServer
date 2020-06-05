using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Common
{
    public interface IMyMemory
    {
        byte[] AsArray();
        IEnumerable<(byte item, int index)> Enumerate();
        ReadOnlyMemory<byte> Slice(int startPosition, int len);
        ReadOnlySpan<byte> Span { get; }
    }


    public struct MyMemoryAsByteArray : IMyMemory
    {
        private readonly byte[] _array;

        public MyMemoryAsByteArray(byte[] array)
        {
            _array = array;
        }

        public byte[] AsArray()
        {
            return _array;
        }

        public IEnumerable<(byte item, int index)> Enumerate()
        {
            var index = 0;
            foreach (var b in _array)
            {
                yield return (b, index);
                index++;
            }
        }

        public ReadOnlyMemory<byte> Slice(int startPosition, int len)
        {
            return new ReadOnlyMemory<byte>(_array).Slice(startPosition, len);
        }

        public ReadOnlySpan<byte> Span => _array;
    }


    public static class MemoryUtils
    {
        public static IMyMemory AsMyMemory(this byte[] src)
        {
            return ChunkedStream.Create(src);
        }
        
        public static IMyMemory AsMyMemory(this ReadOnlyMemory<byte> src)
        {
            return ChunkedStream.Create(src);
        }
        

        
    }
}