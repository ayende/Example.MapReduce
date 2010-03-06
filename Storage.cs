using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace MapReduce
{
    public class Storage : IStorage
    {
        private readonly ConcurrentDictionary<string, List<object>> resultsByBatchId =
            new ConcurrentDictionary<string, List<object>>();

        private readonly ConcurrentDictionary<int, HashSet<string>> resultsByLevel =
            new ConcurrentDictionary<int, HashSet<string>>();

        #region IStorageService Members

        public void Store(int batchId, object value, int level)
        {
            var bag = resultsByBatchId.GetOrAdd(level + "." + batchId, guid => new List<object>());
            var batches = resultsByLevel.GetOrAdd(level, new HashSet<string>());
            batches.Add(level + "." + batchId);
            bag.Add(value);
        }

        public IEnumerable<IEnumerable<object>> GetBatchesFor(int level)
        {
            HashSet<string> batchesIds;
            if (resultsByLevel.TryGetValue(level, out batchesIds) == false)
                throw new InvalidOperationException("Could not find level: " + level);
            return batchesIds.Select(batchId => resultsByBatchId[batchId]);
        }

        #endregion
    }
}