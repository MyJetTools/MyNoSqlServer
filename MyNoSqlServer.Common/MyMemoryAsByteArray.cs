using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Common
{
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

}