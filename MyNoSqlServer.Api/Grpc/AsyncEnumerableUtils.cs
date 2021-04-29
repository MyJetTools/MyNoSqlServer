using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MyNoSqlServer.Api.Grpc
{

    public class AsyncEnumerator<T> : IAsyncEnumerator<T>
    {

        private readonly IEnumerator<T> _enumerator;

        public AsyncEnumerator(IEnumerator<T> enumerator)
        {
            _enumerator = enumerator;
        }
        
        
        public ValueTask DisposeAsync()
        {
            return new ();
        }

        public ValueTask<bool> MoveNextAsync()
        {
            return new (_enumerator.MoveNext());
        }

        public T Current => _enumerator.Current;
    }
    

    public class AsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        private readonly IEnumerable<T> _enumerable;

        public AsyncEnumerable(IEnumerable<T> enumerable)
        {
            _enumerable = enumerable;
        }
        
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = new ())
        {
            return new AsyncEnumerator<T>(_enumerable.GetEnumerator());
        }
    }
    
    public static class AsyncEnumerableUtils
    {
        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> src)
        {
            return new AsyncEnumerable<T>(src ?? Array.Empty<T>());
        }
    }
    
}