using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
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
        private readonly ISettingsLocation _settingsLocation;

        public MasterNodeSaver(IMyNoSqlServerNodePersistenceGrpcService myNoSqlServerNodePersistenceGrpcService, 
            PersistenceQueue persistenceQueue, AppLogs appLogs, IMyNoSqlNodePersistenceSettings persistenceSettings, ISettingsLocation settingsLocation)
        {
            _myNoSqlServerNodePersistenceGrpcService = myNoSqlServerNodePersistenceGrpcService;
            _persistenceQueue = persistenceQueue;
            _appLogs = appLogs;
            _persistenceSettings = persistenceSettings;
            _settingsLocation = settingsLocation;
        }



        private (byte[] payload, bool compressed) CompilePayload(Dictionary<string, List<ITransactionEvent>> events)
        {
            var model = new List<SyncTransactionGrpcModel>();
            
            foreach (var (_, transactionEvents) in events)
            {

                foreach (var transactionEvent in transactionEvents)
                {
                    var grpcModel = transactionEvent.ToSyncTransactionGrpcModel(_settingsLocation.Location);   
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
                var compressedPayload = MyNoSqlServerDataCompression.ZipPayload(payload);
                
                Console.WriteLine($"Compressed size: {compressedPayload.Length}");

                return compressedPayload.Length < payload.Length ? (compressedPayload, true) : (payload, false);
            }


            return (payload, false);



        }


        private async Task SaveToMasterNodeAsync(Dictionary<string, List<ITransactionEvent>> events)
        {
            while (true)
            {
                try
                {

                    var (payload, compressed) = CompilePayload(events);

                    if (compressed)
                    {
                        await _myNoSqlServerNodePersistenceGrpcService.SyncTransactionsCompressedAsync(payload.SplitAndWrap(_persistenceSettings.MaxPayloadSize));
                    }
                    else
                    {
                        await _myNoSqlServerNodePersistenceGrpcService.SyncTransactionsAsync(payload.SplitAndWrap(_persistenceSettings.MaxPayloadSize));
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