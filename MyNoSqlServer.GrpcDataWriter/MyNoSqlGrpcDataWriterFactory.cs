using System;
using Grpc.Net.Client;
using MyNoSqlServer.Grpc;
using ProtoBuf.Grpc.Client;

namespace MyNoSqlServer.GrpcDataWriter
{
    public static class MyNoSqlGrpcDataWriterFactory
    {
        public static MyNoSqlGrpcDataWriter Create(string grpcUrl)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            var channel = GrpcChannel.ForAddress(grpcUrl).CreateGrpcService<IMyNoSqlTransportGrpcService>();

            return new MyNoSqlGrpcDataWriter(channel);
        }
    }
}