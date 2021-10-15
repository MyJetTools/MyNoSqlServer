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

}