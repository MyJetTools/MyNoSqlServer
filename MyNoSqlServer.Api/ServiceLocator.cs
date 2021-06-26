using System;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DotNetCoreDecorators;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Api.DataReadersTcpServer;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.AzureStorage;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.DataReadersBroadcast;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Logs;
using MyNoSqlServer.Domains.Nodes;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.Persistence.Blobs;
using MyNoSqlServer.Domains.Persistence.MasterNode;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.Api
{
    public static class ServiceLocator
    {
        static ServiceLocator()
        {
            StartedAt = DateTime.UtcNow;

            var name = Assembly.GetEntryAssembly()?.GetName();

            string appName = name?.Name ?? string.Empty;

            var nameSegments = appName.Split('.', StringSplitOptions.RemoveEmptyEntries);

            if (nameSegments.Length > 2)
            {
                appName = string.Join('.', nameSegments.Skip(1));
            }

            AppName = appName;
            AppVersion = name?.Version?.ToString();

            AspNetEnvironment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            Host = Environment.GetEnvironmentVariable("HOSTNAME");

            Console.WriteLine($"AppName: {AppName}");
            Console.WriteLine($"AppVersion: {AppVersion}");
            Console.WriteLine($"AspNetEnvironment: {AspNetEnvironment}");
            Console.WriteLine($"Host: {Host}");
            Console.WriteLine($"StartedAt: {StartedAt}");
            Console.WriteLine($"Port http: 5123");
            Console.WriteLine($"Port tcp: 5125");
            Console.WriteLine();
        }

        public static string AppName { get; }
        public static string AppVersion { get;  }

        public static DateTime StartedAt { get; }

        public static string AspNetEnvironment { get; }

        public static string Host { get; }

        public static DbInstance DbInstance { get; private set; }
        
        public static GlobalVariables GlobalVariables { get; private set; }
        
        public static DataReadersTcpBroadcaster DataReadersTcpBroadcaster { get; private set; }
        
        public static NodeSessionsList NodeSessionsList { get; private set; }
        
        public static DataInitializer DataInitializer { get; private set; }
        
        public static AppLogs AppLogs { get; private set; }
        
        public static SyncTransactionHandler SyncTransactionHandler { get; private set; }


        private static BlobsSaver _blobsSaver;

        private static MasterNodeSaver _masterNodeSaver;

        public static DbOperations DbOperations { get; private set; }
        
        public static PersistenceQueue PersistenceQueue { get; private set; }
        
        
        public static NodeClient NodeClient { get; private set; }
        
        
        public static readonly MyServerTcpSocket<IMyNoSqlTcpContract> TcpServer = 
            new MyServerTcpSocket<IMyNoSqlTcpContract>(new IPEndPoint(IPAddress.Any, 5125))
                .RegisterSerializer(()=> new MyNoSqlTcpSerializer())
                .SetService(()=>new DataReaderTcpService())
                .Logs.AddLogInfo((c, msg)=>
                {
                    AppLogs.WriteInfo(null, "TcpLog", c == null ? "*:5125" : $"*:5125. ConnectionId:{c.Id}", msg);
                })
                .Logs.AddLogException((c, e)=>
                {
                    AppLogs.WriteError(null, "TcpLog", 
                        c == null
                            ? "*:5125" 
                            : $"*:5125. ConnectionId:{c.Id}", 
                        e);
                });


        private static readonly TaskTimer TimerSaver = new (TimeSpan.FromSeconds(1));
        private static readonly TaskTimer NodesTimer = new (TimeSpan.FromSeconds(5));

        private static readonly TaskTimer TimerOneMinute = new (TimeSpan.FromMinutes(1));

        public static readonly PostTransactionsList PostTransactionsList = new();

        public static void Init(IServiceProvider sp)
        {
            

            AzureStorageBinder.Init(sp);
            
            sp.LinkDomainServices();
            
            DbInstance = sp.GetRequiredService<DbInstance>();
            
            GlobalVariables = sp.GetRequiredService<GlobalVariables>();

            DbOperations = sp.GetRequiredService<DbOperations>();

            var persistenceShutdown = sp.GetRequiredService<IPersistenceShutdown>();
            _blobsSaver =  persistenceShutdown as BlobsSaver;
            _masterNodeSaver = persistenceShutdown as MasterNodeSaver;


            DataReadersTcpBroadcaster = (DataReadersTcpBroadcaster)sp.GetRequiredService<IDataReadersBroadcaster>();

            NodeSessionsList = sp.GetRequiredService<NodeSessionsList>();

            DataInitializer = sp.GetRequiredService<DataInitializer>();

            AppLogs = sp.GetRequiredService<AppLogs>();

            SyncTransactionHandler = sp.GetRequiredService<SyncTransactionHandler>();

            PersistenceQueue = sp.GetRequiredService<PersistenceQueue>();
            
            NodeClient = sp.GetService<NodeClient>();

        }

        public static void Start(IServiceProvider sp, SettingsModel settingsModel)
        {
            if (settingsModel.IsNode())
            {
                AppLogs.WriteInfo(null, "Start", null, "Plugged Grpc Node Client");
                NodeClient.Start();
            }
            else
            {
                DataInitializer.LoadSnapshotsAsync(sp.GetRequiredService<ITablesPersistenceReader>()).Wait();
            }


            TimerOneMinute.Register("GC transactions", () =>
            {
                PostTransactionsList.GcTransactions();
                return new ValueTask();
            });
            
            TimerOneMinute.Register("GC TableRecords", () =>
            {
                DbOperations.Gc();
                return new ValueTask();
            });


            if (_blobsSaver != null)
            {
                AppLogs.WriteInfo(null, "Start", null, "Plugged BlobSaver as persistence handler");
                TimerSaver.Register("Persist", _blobsSaver.FlushToBlobAsync);    
            }

            if (_masterNodeSaver != null)
            {
                AppLogs.WriteInfo(null, "Start", null, "Plugged MasterNodeSaver as persistence handler");
                TimerSaver.Register("Persist", _masterNodeSaver.FlushDataAsync);    
            }

            
            TimerSaver.RegisterExceptionHandler((msg, e) =>
            {
                AppLogs.WriteError(null, "TimerSaver", msg, e);
                return new ValueTask();
            });

            NodesTimer.Register("NodeSessions GC", () =>
            {
                NodeSessionsList.Gc();
                return new ValueTask();
            });
            
            NodesTimer.Register("NodeSessions Ping", () =>
            {
                NodeSessionsList.SendPing();
                return new ValueTask();
            });
            
            TimerOneMinute.Start();
            
            TimerSaver.Start();
            TcpServer.Start();
            NodesTimer.Start();
        }

        public static void Stop()
        {
            Console.WriteLine("Shutting down the application. Delaying 500ms");
            GlobalVariables.IsShuttingDown = true;
            Thread.Sleep(500);
            
            Console.WriteLine("Stopping Snapshot Saver Engine gracefully");
            DataInitializer.StopAsync().Wait();
        }
    }
}