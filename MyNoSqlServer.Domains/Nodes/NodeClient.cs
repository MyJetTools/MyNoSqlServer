using System;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
{
    public class NodeClient
    {
        private readonly DbInstance _dbInstance;
        private readonly IMyNoSqlServerNodeSynchronizationGrpcService _grpcService;
        private readonly ISettingsLocation _settingsLocation;

        private readonly string _sessionId;

        public NodeClient(DbInstance dbInstance,
            IMyNoSqlServerNodeSynchronizationGrpcService grpcService, ISettingsLocation settingsLocation)
        {
            _dbInstance = dbInstance;
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
                    var result = await _grpcService.SyncAsync(new SyncGrpcRequest
                    {
                        Location = _settingsLocation.Location,
                        RequestId = requestId,
                        SessionId = _sessionId
                    });

                    HandleSyncEvent(result);

                    requestId++;

                }
                catch (Exception e)
                {
                    Console.WriteLine("NodeClient: "+e);
                }
                
            }
            
        }


        private void HandleSyncEvent(SyncGrpcResponse syncGrpcResponse)
        {
            
            if (syncGrpcResponse.TableName == null)
                return;

            var table =  _dbInstance.GetTable(syncGrpcResponse.TableName);

            if (syncGrpcResponse.TableAttributes != null)
            {
                
            }
            
            //ToDo - сделать синхронизацию данных
            
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