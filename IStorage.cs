using System.Collections.Generic;

namespace MapReduce
{
    public interface IStorage
    {
        void Store(int batchId, object value, int level);
        IEnumerable<IEnumerable<object>> GetBatchesFor(int level);
    }
}