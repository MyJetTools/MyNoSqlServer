using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
{
    public class NodeClient
    {
        private readonly SyncTransactionHandler _syncTransactionHandler;
        private readonly IMyNoSqlServerNodeSynchronizationGrpcService _grpcService;
        private readonly ISettingsLocation _settingsLocation;

        private readonly string _sessionId;

        public NodeClient(SyncTransactionHandler syncTransactionHandler,
            IMyNoSqlServerNodeSynchronizationGrpcService grpcService, ISettingsLocation settingsLocation)
        {
            _syncTransactionHandler = syncTransactionHandler;
            _grpcService = grpcService;
            _settingsLocation = settingsLocation;
            _sessionId = Guid.NewGuid().ToString("N");
        }


        private async Task ThreadLoopAsync()
        {

            long requestId = 0;

            while (_working)
            {
                try
                {
                    var grpcResponse = await _grpcService.SyncAsync(new SyncGrpcRequest
                    {
                        Location = _settingsLocation.Location,
                        RequestId = requestId,
                        SessionId = _sessionId
                    });

                    _syncTransactionHandler.HandleTransaction(grpcResponse, () =>
                        CreateTransactionEventAttribute(grpcResponse.Locations, grpcResponse.Headers));

                    requestId++;

                }
                catch (Exception e)
                {
                    Console.WriteLine("NodeClient: "+e);
                }
                
            }
            
        }


        private TransactionEventAttributes CreateTransactionEventAttribute(List<string> locations, SyncGrpcHeader[] headers)
        {
            locations.Add(_settingsLocation.Location);
            return new TransactionEventAttributes(locations,
                DataSynchronizationPeriod.Sec5,
                EventSource.Synchronization,
                headers.ToDictionary(itm => itm.Key, itm => itm.Value)
            );
        }





        private bool _working;
        private Task _task;


        public void Start()
        {
            _working = true;
            _task = ThreadLoopAsync();

        }
    }
}