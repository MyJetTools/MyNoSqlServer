using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Client;
using MyNoSqlClient.ReadRepository;

namespace MyNoSqlClient.SignalR
{
    public class MyNoSqlSignalRConnection : MyNoSqlSubscriber, IMySignalRConnection
    {

        private const string PathForSubscribes = "changes";


        private readonly string _signalRurl;
        private readonly TimeSpan _pingTimeOut;

        private readonly HubConnectionSynchronizer _currentConnection = new HubConnectionSynchronizer();

        public MyNoSqlSignalRConnection(string url, TimeSpan pingTimeOut)
        {
            Url = url;
            _signalRurl = url.Last() == '/' ? url + PathForSubscribes : url + "/" + PathForSubscribes;
            _pingTimeOut = pingTimeOut;
        }

        public MyNoSqlSignalRConnection(string url) :
            this(url, TimeSpan.FromSeconds(30))
        {
        }


        public string Url { get; }

        
        private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _requests 
            = new ConcurrentDictionary<string, TaskCompletionSource<string>>();


        private async Task SubscribeAsync(HubConnection hubConnection)
        {

            hubConnection.On<string>(SystemAction, action =>
            {
                if (action == "heartbeat")
                    _lastIncomingDateTime = DateTime.UtcNow;
            });

            hubConnection.On<string>("TableNotFound", corrId =>
            {
                if (_requests.TryRemove(corrId, out var taskCompletion))
                {
                    taskCompletion.SetException(new Exception("Table not found"));
                }
            });
            
            hubConnection.On<string>("RowNotFound", corrId =>
            {
                if (_requests.TryRemove(corrId, out var taskCompletion))
                {
                    taskCompletion.SetResult(null);
                }
            });
            
            hubConnection.On<string, byte[]>("Row", (corrId, data) =>
            {
                if (_requests.TryRemove(corrId, out var taskCompletion))
                {
                    taskCompletion.SetResult(Encoding.UTF8.GetString(data));
                }
            });

            
            hubConnection.On<string, byte[]>("Rows", (corrId, data) =>
            {
                if (_requests.TryRemove(corrId, out var taskCompletion))
                {
                    taskCompletion.SetResult(Encoding.UTF8.GetString(data));
                }
            });

            
            foreach (var tableName in Deserializers.Keys)
            {
                hubConnection.On<string, byte[]>(tableName, (action, data) =>
                {
                    _lastIncomingDateTime = DateTime.UtcNow;

                    switch (action)
                    {
                        case "i":
                            HandleInitTableEvent(tableName, data);
                            break;
                        case "u":
                            HandleUpdateRowEvent(tableName, data);
                            break;
                        case "d":
                            var json = Encoding.UTF8.GetString(data);
                            var items = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
                            HandleDeleteRowEvent(tableName, items.Select(itm => (itm.Key, itm.Value)));
                            break;
                        default:
                        {
                            if (action.StartsWith("i:"))
                                HandleInitPartitionEvent(tableName, action.Substring(2, action.Length - 2), data);
                            break;
                        }
                    }
                });
                

                Console.WriteLine("Subscribed to MyNoSql Server table: " + tableName);
                await hubConnection.SendAsync("Subscribe", tableName);
            }

        }


        private async Task StartAsync(HubConnection hubConnection)
        {

            while (true)
            {
                try
                {

                    Console.WriteLine("Connecting to MyNoSql Server using SignalR: " + _signalRurl);
                    await hubConnection.StartAsync();
                    Console.WriteLine("Connected to MyNoSql Server using SignalR");
                    _lastIncomingDateTime = DateTime.UtcNow;
                    return;

                }
                catch (Exception e)
                {
                    Console.WriteLine($"MyNoSql SignalR connection error to {_signalRurl}. Error: " + e.Message);
                    await Task.Delay(1000);
                }
            }

        }


        private async Task PingProcessAsync(HubConnection hubConnection)
        {
            while (DateTime.UtcNow - _lastIncomingDateTime < _pingTimeOut && hubConnection.State == HubConnectionState.Connected)
            {
                await Task.Delay(TimeSpan.FromSeconds(5));
                await hubConnection.SendAsync("Ping");
            }
            Console.WriteLine($"Disconnected: Last incoming packet has been: {(DateTime.UtcNow - _lastIncomingDateTime).TotalSeconds} ms ago. Connection state: {HubConnectionState.Connected}");

        }

        private DateTime _lastIncomingDateTime;

        private async Task TheTask()
        {
            while (_started)
            {
                try
                {
                    var hubConnection = new HubConnectionBuilder()
                        .WithUrl(_signalRurl)
                        .Build();

                    await StartAsync(hubConnection);
                    
                    _currentConnection.Set(hubConnection);
                    
                    await SubscribeAsync(hubConnection);
                    await PingProcessAsync(hubConnection);
                    await hubConnection.StopAsync();
                    _currentConnection.Set(null);
                    
                    ResponseAsAllRequestsAreDisconnected();

                }
                catch (Exception e)
                {
                    Console.WriteLine("TheTask:" + e);
                }
            }
        }


        private void ResponseAsAllRequestsAreDisconnected()
        {
            var keys = _requests.Keys;
            foreach (var key in keys)
            {
                if (_requests.TryRemove(key, out var taskCompletion))
                    taskCompletion.SetException(new Exception("Socket is disconnected"));
                
            }
        }

        private Task _task;

        private bool _started;

        public void Start()
        {

            _started = true;
            _task = TheTask();
        }

        public void Stop()
        {
            _started = false;
            _task.Wait();
        }
    }
}