using System;
using System.Net;
using MyNoSqlClient.ReadRepository;
using MyTcpSockets;
using IMyNoSqlTcpContract = MyNoSqlClient.Tcp.Contracts.IMyNoSqlTcpContract;
using MyNoSqlTcpSerializer = MyNoSqlClient.Tcp.Contracts.MyNoSqlTcpSerializer;

namespace MyNoSqlClient.Tcp
{
    public class MyNoSqlTcpClient : MyNoSqlSubscriber
    {

        private readonly MyClientTcpSocket<IMyNoSqlTcpContract> _tcpClient;
        
        
        public MyNoSqlTcpClient(IPEndPoint ipEndPoint)
        {
            _tcpClient = new MyClientTcpSocket<IMyNoSqlTcpContract>(ipEndPoint, TimeSpan.FromSeconds(3));
  
            _tcpClient
                .RegisterTcpContextFactory(() => new MyNoSqlServerClientTcpContext(this))
                .AddLog((m)=> Console.WriteLine("MyNoSql: "+m))
                .RegisterTcpSerializerFactory(() => MyNoSqlTcpSerializer.Instance);
        }

        public void Start()
        {
            _tcpClient.Start();
        }
        
        public void Stop()
        {
            _tcpClient.Stop();
        }
    }
}