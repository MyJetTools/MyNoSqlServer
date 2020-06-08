using System;
using System.Linq;
using System.Net;
using System.Reflection;
using MyDependencies;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db;
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
        }

        public static string AppName { get; private set; }
        public static string AppVersion { get; private set; }

        public static DateTime StartedAt { get; private set; }

        public static string AspNetEnvironment { get; private set; }

        public static string Host { get; private set; }

        public static DbInstance DbInstance { get; private set; }
        
        public static GlobalVariables GlobalVariables { get; private set; }

        public static ISnapshotStorage SnapshotStorage { get; private set; }
        
        public static IReplicaSynchronizationService DataSynchronizer { get; private set; }

        public static SnapshotSaverEngine SnapshotSaverEngine { get; private set; }
        
        public static readonly ISnapshotSaverScheduler SnapshotSaverScheduler = new SnapshotSaverScheduler();
        
        public static DbOperations DbOperations { get; private set; }
        
        public static readonly MyServerTcpSocket<IMyNoSqlTcpContract> TcpServer = 
            new MyServerTcpSocket<IMyNoSqlTcpContract>(new IPEndPoint(IPAddress.Any, 5125))
                .RegisterSerializer(()=> new MyNoSqlTcpSerializer())
                .SetService(()=>new ChangesTcpService())
                .AddLog(Console.WriteLine);

        public static void Init(IServiceResolver sr)
        {
            DbInstance = sr.GetService<DbInstance>();
            SnapshotStorage = sr.GetService<ISnapshotStorage>();
            
            DataSynchronizer = new ChangesPublisherToSocket();
            SnapshotSaverEngine = sr.GetService<SnapshotSaverEngine>();

            GlobalVariables = sr.GetService<GlobalVariables>();

            DbOperations = sr.GetService<DbOperations>();
        }

        public static void Start()
        {
            SnapshotSaverEngine.Start();
            TcpServer.Start();
        }
    }
}