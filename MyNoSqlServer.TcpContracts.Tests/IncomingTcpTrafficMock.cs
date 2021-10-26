using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts.Tests
{
    public class IncomingTcpTrafficMock : IIncomingTcpTrafficReader
    {

        public readonly Queue<byte> IncomingTraffic = new Queue<byte>();


        public void NewPackageAsync(ReadOnlyMemory<byte> incoming)
        {
            var span = incoming.Span;
            foreach (var b in span)
            {
                IncomingTraffic.Enqueue(b);
            }

        }

        public async ValueTask<byte> ReadByteAsync(CancellationToken token)
        {
            while (true)
            {
                if (IncomingTraffic.Count > 0)
                    return IncomingTraffic.Dequeue();

                await Task.Delay(100, token);
            }



        }

        public ValueTask<int> ReadBytesAsync(Memory<byte> buffer, CancellationToken token)
        {

            var pos = 0;

            var span = buffer.Span;

            while (pos<buffer.Length)
            {

                if (IncomingTraffic.Count == 0)
                    return new ValueTask<int>(pos);

                var b= IncomingTraffic.Dequeue();
                span[pos] = b;
                pos++;
            }

            return new ValueTask<int>(pos);

        }

    }
}