using System;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using DotNetCoreDecorators;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.SnapshotSaver;
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
        
        public static IReplicaSynchronizationService DataSynchronizer { get; private set; }

        private static SnapshotSaverEngine _snapshotSaverEngine;
        
        public static  ISnapshotSaverScheduler SnapshotSaverScheduler { get; private set; }
        
        public static PersistenceHandler PersistenceHandler { get; private set; }
        public static DbOperations DbOperations { get; private set; }
        
        public static readonly MyServerTcpSocket<IMyNoSqlTcpContract> TcpServer = 
            new MyServerTcpSocket<IMyNoSqlTcpContract>(new IPEndPoint(IPAddress.Any, 5125))
                .RegisterSerializer(()=> new MyNoSqlTcpSerializer())
                .SetService(()=>new ChangesTcpService())
                .Logs.AddLogInfo((c, d)=>
                {
                    if (c == null)
                        Console.WriteLine($"DateTime: {DateTime.UtcNow}. "+d);
                    else
                        Console.WriteLine($"DateTime: {DateTime.UtcNow}. ConnectionId:{c.Id}. "+d);
                })
                .Logs.AddLogException((c, d)=>
                {
                    if (c == null)
                        Console.WriteLine($"DateTime: {DateTime.UtcNow}. "+d);
                    else
                        Console.WriteLine($"DateTime: {DateTime.UtcNow}. ConnectionId:{c.Id}. "+d);
                });


        private static readonly TaskTimer TimerSaver = new TaskTimer(TimeSpan.FromSeconds(1));

        public static readonly MultiPartGetSnapshots MultiPartGetSnapshots = new();

        public static readonly PostTransactionsList PostTransactionsList = new();

        public static void Init(ServiceProvider sr)
        {
            DbInstance = sr.GetRequiredService<DbInstance>();
            
            DataSynchronizer = new ChangesPublisherToSocket();
            _snapshotSaverEngine = sr.GetRequiredService<SnapshotSaverEngine>();

            GlobalVariables = sr.GetRequiredService<GlobalVariables>();

            DbOperations = sr.GetRequiredService<DbOperations>();

            PersistenceHandler = sr.GetRequiredService<PersistenceHandler>();

            SnapshotSaverScheduler = sr.GetRequiredService<ISnapshotSaverScheduler>();

        }

        public static void Start()
        {
            
            _snapshotSaverEngine.LoadSnapshotsAsync().Wait();

            TimerSaver.Register("Persist", ()=> _snapshotSaverEngine.IterateAsync(GlobalVariables.IsShuttingDown));
            TimerSaver.Start();
            TcpServer.Start();
        }

        public static void Stop()
        {
            Console.WriteLine("Shutting down the application. Delaying 500ms");
            GlobalVariables.IsShuttingDown = true;
            Thread.Sleep(500);
            
            Console.WriteLine("Stopping Snapshot Saver Engine gracefully");
            _snapshotSaverEngine.StopAsync().Wait();
        }
    }
}