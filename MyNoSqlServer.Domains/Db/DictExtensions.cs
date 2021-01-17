using System.Collections.Generic;

namespace MyNoSqlServer.Domains.Db
{
    public static class DictExtensions
    {

        public static Dictionary<TKey, TValue> AddByCreatingNewDictionary<TKey, TValue>(
            this Dictionary<TKey, TValue> dict, TKey key, TValue value)
        {

            var result = new Dictionary<TKey, TValue>(dict) {{key, value}};
            return result;
        }
        
        public static Dictionary<TKey, TValue> RemoveByCreatingNewDictionary<TKey, TValue>(
            this Dictionary<TKey, TValue> dict, TKey key)
        {
            var result = new Dictionary<TKey, TValue>(dict);
            result.Remove(key);
            return result;
        }
        
    }
}