using System;
using Grpc.Net.Client;
using MyNoSqlServer.Grpc;
using ProtoBuf.Grpc.Client;

namespace MyNoSqlServer.GrpcDataWriter
{
    public static class MyNoSqlGrpcDataWriterFactory
    {
        public static MyNoSqlGrpcDataWriter CreateNoSsl(string grpcUrl, Action<GrpcChannel> adjustChannel = null)
        {
            GrpcClientFactory.AllowUnencryptedHttp2 = true;

            var channel = GrpcChannel.ForAddress(grpcUrl);

            adjustChannel?.Invoke(channel);

            return new MyNoSqlGrpcDataWriter(channel.CreateGrpcService<IMyNoSqlWriterGrpcService>());
        }
    }
}