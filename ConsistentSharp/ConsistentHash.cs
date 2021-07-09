using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ConsistentSharp
{
    /// from https://github.com/stathat/consistent/blob/master/consistent.go
    public partial class ConsistentHash : IDisposable
    {
        public SortedList<uint, string> Circle { get; } = new SortedList<uint, string>();
        private readonly SortedList<string, bool> members = new SortedList<string, bool>();
        private readonly ReaderWriterLockSlim rwlock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private readonly IHashAlgorithm hashAlgorithm;

        public long Count { get; private set; }

        public int NumberOfReplicas { get; set; } = 20;

        public ConsistentHash(IHashAlgorithm hashAlgorithm = null)
        {
            this.hashAlgorithm = hashAlgorithm ?? new Crc32HashAlgorithm();
        }

        public IEnumerable<string> Members
        {
            get
            {
                rwlock.EnterReadLock();

                try
                {
                    return members.Keys.ToArray();
                }
                finally
                {
                    rwlock.ExitReadLock();
                }
            }
        }

        public void Dispose()
        {
            rwlock.Dispose();
        }


        public void Add(string elt)
        {
            if (elt == null)
            {
                throw new ArgumentNullException(nameof(elt));
            }

            rwlock.EnterWriteLock();

            try
            {
                _Add(elt);
            }
            finally
            {
                rwlock.ExitWriteLock();
            }
        }

        private void _Add(string elt)
        {
            for (var i = 0; i < NumberOfReplicas; i++)
            {
                Circle[hashAlgorithm.HashKey(EltKey(elt, i))] = elt;
            }

            members[elt] = true;
            
            Count++;
        }

        public void Remove(string elt)
        {
            if (elt == null)
            {
                throw new ArgumentNullException(nameof(elt));
            }

            rwlock.EnterWriteLock();
            try
            {
                _Remove(elt);
            }
            finally
            {
                rwlock.ExitWriteLock();
            }
        }

        private void _Remove(string elt)
        {
            for (var i = 0; i < NumberOfReplicas; i++)
            {
                Circle.Remove(hashAlgorithm.HashKey(EltKey(elt, i)));
            }

            members.Remove(elt);
            
            Count--;
        }

        public void Set(IEnumerable<string> elts)
        {
            if (elts == null)
            {
                throw new ArgumentNullException(nameof(elts));
            }

            _Set(elts.ToArray());
        }

        private void _Set(string[] elts)
        {
            rwlock.EnterWriteLock();
            try
            {
                foreach (var k in members.Keys.ToArray())
                {
                    var found = elts.Any(v => k == v);

                    if (!found)
                    {
                        _Remove(k);
                    }
                }

                foreach (var v in elts)
                {
                    if (members.ContainsKey(v))
                    {
                        continue;
                    }

                    _Add(v);
                }
            }

            finally
            {
                rwlock.ExitWriteLock();
            }
        }

        public string Get(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            rwlock.EnterReadLock();

            try
            {
                if (Count == 0)
                {
                    throw new EmptyCircleException();
                }

                var key = hashAlgorithm.HashKey(name);

                var i = Search(key);

                return Circle.Values[i];
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        public (string, string) GetTwo(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            rwlock.EnterReadLock();

            try
            {
                if (Count == 0)
                {
                    throw new EmptyCircleException();
                }

                var key = hashAlgorithm.HashKey(name);

                var i = Search(key);

                var a = Circle.Values[i];

                if (Count == 1)
                {
                    return (a, default(string));
                }

                var start = i;

                var b = default(string);

                for (i = start + 1; i != start; i++)
                {
                    if (i >= Circle.Count)
                    {
                        i = 0;
                    }

                    b = Circle.Values[i];

                    if (b != a)
                    {
                        break;
                    }
                }

                return (a, b);
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        public IEnumerable<string> GetN(string name, int n)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (Count < n)
            {
                n = (int) Count;
            }

            rwlock.EnterReadLock();

            try
            {
                if (Count == 0)
                {
                    throw new EmptyCircleException();
                }


                var key = hashAlgorithm.HashKey(name);
                var i = Search(key);
                var start = i;
                var res = new List<string>();
                var elem = Circle.Values[i];

                res.Add(elem);
                if (res.Count == n)
                {
                    return res;
                }

                for (i = start + 1; i != start; i++)
                {
                    if (i >= Circle.Count)
                    {
                        i = 0;
                    }

                    elem = Circle.Values[i];

                    if (!res.Contains(elem))
                    {
                        res.Add(elem);
                    }

                    if (res.Count == n)
                    {
                        break;
                    }
                }

                return res;
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        private int Search(uint key)
        {
            var i = BinarySearch(Circle.Count, x => Circle.Keys[x] > key);

            if (i >= Circle.Count)
            {
                i = 0;
            }

            return i;
        }

        /// Search uses binary search to find and return the smallest index i in [0, n) at which f(i) is true
        /// golang sort.Search
        private static int BinarySearch(int n, Func<int, bool> f)
        {
            var s = 0;
            var e = n;

            while (s < e)
            {
                var m = s + (e - s) / 2;

                if (!f(m))
                {
                    s = m + 1;
                }
                else
                {
                    e = m;
                }
            }

            return s;
        }

        private static string EltKey(string elt, int idx)
        {
            return $"{idx}{elt}";
        }
    }
}