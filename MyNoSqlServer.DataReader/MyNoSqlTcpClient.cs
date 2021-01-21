using System;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.DataReader
{
    public class MyNoSqlTcpClient : MyNoSqlSubscriber
    {

        private readonly MyClientTcpSocket<IMyNoSqlTcpContract> _tcpClient;
        
        public MyNoSqlTcpClient(Func<string> getHostPort, string appName)
        {
            _tcpClient = new MyClientTcpSocket<IMyNoSqlTcpContract>(getHostPort, TimeSpan.FromSeconds(3));

            _tcpClient
                .RegisterTcpContextFactory(() => new MyNoSqlServerClientTcpContext(this, appName))
                .AddLog((c, m) => Console.WriteLine("MyNoSql: " + m))
                .RegisterTcpSerializerFactory(() => new MyNoSqlTcpSerializer());
        }


        public bool Connected => _tcpClient.Connected;

        public long ConnectionId
        {
            get
            {
                var currentContext = _tcpClient.CurrentTcpContext;

                if (currentContext == null)
                    return -1;

                return currentContext.Id;
            }
        }

        public void Start()
        {
            _tcpClient.Start();
        }
        
        public void Stop()
        {
            _tcpClient.Stop();
        }

        public override void UpdateExpirationDate(string tableName, string partitionKey, string[] rowKeys, 
            DateTime? expirationTime,
            bool cleanExpirationTime)
        {
            var ctx = _tcpClient.CurrentTcpContext;
            
            if (ctx == null)
                return;


            var contract = new UpdateExpiresTimeTcpContract
            {
                TableName = tableName,
                PartitionKey = partitionKey,
                RowKeys = rowKeys,
                Expires = expirationTime,
            };
            
            ctx.SendDataToSocket(contract);
        }
    }
}