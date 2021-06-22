using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.DataCompression;
using MyNoSqlServer.Domains.Logs;
using MyNoSqlServer.Domains.Nodes;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Persistence.MasterNode
{
    public class MasterNodeSaver : IPersistenceShutdown
    {
        private readonly IMyNoSqlServerNodePersistenceGrpcService _myNoSqlServerNodePersistenceGrpcService;
        private readonly PersistenceQueue _persistenceQueue;
        private readonly AppLogs _appLogs;
        private readonly IMyNoSqlNodePersistenceSettings _persistenceSettings;

        public MasterNodeSaver(IMyNoSqlServerNodePersistenceGrpcService myNoSqlServerNodePersistenceGrpcService, 
            PersistenceQueue persistenceQueue, AppLogs appLogs, IMyNoSqlNodePersistenceSettings persistenceSettings)
        {
            _myNoSqlServerNodePersistenceGrpcService = myNoSqlServerNodePersistenceGrpcService;
            _persistenceQueue = persistenceQueue;
            _appLogs = appLogs;
            _persistenceSettings = persistenceSettings;
        }



        private IEnumerable<PayloadWrapperGrpcModel> CompilePayload(Dictionary<string, List<ITransactionEvent>> events)
        {
            var model = new List<SyncTransactionGrpcModel>();
            
            foreach (var (_, transactionEvents) in events)
            {

                foreach (var transactionEvent in transactionEvents)
                {
                    var grpcModel = transactionEvent.ToSyncTransactionGrpcModel();   
                    model.Add(grpcModel); 
                }
            }


            var memStream = new MemoryStream();
            ProtoBuf.Serializer.Serialize(memStream, model);

            var payload = memStream.ToArray();
            
            
            //ToDo - Remove Debug logs
            Console.WriteLine($"Non compressed size: {payload.Length}");

            if (_persistenceSettings.CompressData)
            {
                payload = MyNoSqlServerDataCompression.ZipPayload(payload);
                
                Console.WriteLine($"Compressed size: {payload.Length}");
            }
                


            return payload.SplitPayload(_persistenceSettings.MaxPayloadSize).Select(chunk => new PayloadWrapperGrpcModel
            {
                Payload = chunk
            });

        }


        private async Task SaveToMasterNodeAsync(Dictionary<string, List<ITransactionEvent>> events)
        {
            while (true)
            {
                try
                {

                    var payload = CompilePayload(events).ToAsyncEnumerable();

                    if (_persistenceSettings.CompressData)
                    {
                        await _myNoSqlServerNodePersistenceGrpcService.SyncTransactionsCompressedAsync(payload);
                    }
                    else
                    {
                        await _myNoSqlServerNodePersistenceGrpcService.SyncTransactionsAsync(payload);
                    }
        
                    return;
                }
                catch (Exception e)
                {
                    _appLogs.WriteError(null, "MasterNodeSaver.FlushToMasterNodeAsync", "N/A", e);
                    await Task.Delay(1000);
                }
            }

        }

        private async Task FlushToMasterNodeAsync(Dictionary<string, List<ITransactionEvent>> events)
        {
            try
            {
                await SaveToMasterNodeAsync(events);
            }

            finally
            {
                HasDataInProcess = false;
            }

        }


        public ValueTask FlushDataAsync()
        {

            HasDataInProcess = true;
            var snapshot = _persistenceQueue.GetSnapshot();

            if (snapshot == null)
            {
                HasDataInProcess = false;
                return new ValueTask();
            }
            
            return new ValueTask(FlushToMasterNodeAsync(snapshot));

        }

        public bool HasDataInProcess { get; private set; }
    }
}