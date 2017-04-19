using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GZipStreamMultithread
{
    public class GZipMultithread
    {
        private ConcurrentStack<Thread> _compressors = new ConcurrentStack<Thread>();

        private ConcurrentQueue<DataBlock> _tasks = new ConcurrentQueue<DataBlock>();
        public int CurrentTaskCount { get { return this._tasks.Count; } }

        private ConcurrentDictionary<int, byte[]> _results = new ConcurrentDictionary<int, byte[]>();
        public int CurrentResultCount { get { return this._results.Count; } }

        public long TaskCount;
        public long ResultCount;

        public int MaxTaskNumber { get; set; }
        public int MaxResultNumber { get; set; }

        public GZipMultithread(CompressionMode mode)
        {
            var maxThreadNumber = Environment.ProcessorCount;
            var taskNumberPerThread = 32;
            this.MaxTaskNumber = taskNumberPerThread * maxThreadNumber * 2;
            var resultNumberScale = 3;
            this.MaxResultNumber = this.MaxTaskNumber * resultNumberScale;
            for (int i = 0; i < maxThreadNumber; i++)
            {
                if (mode == CompressionMode.Compress)
                {
                    var compressor = new Thread(Compress);
                    compressor.Name = "compressor" + i.ToString();
                    _compressors.Push(compressor);
                    compressor.Start();
                }
                if (mode == CompressionMode.Decompress)
                {
                    var decompressor = new Thread(Decompress);
                    decompressor.Name = "decompressor" + i.ToString();
                    _compressors.Push(decompressor);
                    decompressor.Start();
                }
            }
            Thread exec = new Thread(Execute);
            exec.Start();
        }


        public void Execute()
        {
            if (this.TaskCount > 0 && this.ResultCount == this.TaskCount)
            {
                // delete all threads
                this.Dispose();
            }
            else
            {
                Thread.Sleep(1000);
            }
        }

        public void AddTask(DataBlock task)
        {
            // TODO : free-lock Dequeue while Enqueue 
            lock (this._tasks)
            {
                _tasks.Enqueue(task);
            }
        }

        private DataBlock GetTask()
        {
            DataBlock result;
            int tryNumber = 0;
            var dequeueResult = _tasks.TryDequeue(out result);
            while (!dequeueResult)
            {
                tryNumber++;
                var sleepTime = 100 * tryNumber * (new Random()).Next(1, 10);
                //Console.WriteLine(Thread.CurrentThread.Name + " Task queue wait " + sleepTime);

                Thread.Sleep(sleepTime);
                dequeueResult = _tasks.TryDequeue(out result);
            }
            return result;
        }

        private void Compress()
        {
            var task = GetTask();
            while (task != null)
            {
                try
                {
                    using (MemoryStream input = new MemoryStream(task.Data))
                    {
                        using (MemoryStream output = new MemoryStream(task.Data.Length))
                        {
                            using (GZipStream cs = new GZipStream(output, CompressionMode.Compress))
                            {
                                cs.Write(task.Data, 0, task.Data.Length);
                            }
                            if (!this.AddResult(new DataBlock(task.ID, output.ToArray())))
                            {
                                this.AddTask(task);
                                this.MaxTaskNumber++;
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    GC.Collect();
                    this.AddTask(task);
                    this.MaxTaskNumber++;
                }
                task = GetTask();
            }
        }

        private void Decompress()
        {
            var task = GetTask();
            while (task != null)
            {
                try
                {
                    using (MemoryStream input = new MemoryStream(task.Data))
                    {
                        using (MemoryStream output = new MemoryStream())
                        {
                            using (GZipStream cs = new GZipStream(input, CompressionMode.Decompress))
                            {
                                cs.CopyTo(output);
                            }
                            if (!this.AddResult(new DataBlock(task.ID, output.ToArray())))
                            {
                                this.AddTask(task);
                                this.MaxTaskNumber++;
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    GC.Collect();
                    this.AddTask(task);
                    this.MaxTaskNumber++;
                }
                task = GetTask();
            }
        }

        private bool AddResult(DataBlock dataBlock)
        {
            while (_results.Count > this.MaxResultNumber)
            {
                if (dataBlock.ID < this._results.Keys.Max())
                {
                    break;
                }
                else
                {
                    return false;
                }
            }
            if (_results.TryAdd(dataBlock.ID, dataBlock.Data))
            {
                this.ResultCount++;
                return true;
            }
            else
            {
                return false;
            }
        }
        public bool GetResultsToWrite(int resultsNumber, int currentSegment, out Dictionary<int, byte[]> results)
        {
            bool result = false;
            results = new Dictionary<int, byte[]>();

            for (int i = 0; i < resultsNumber; i++)
            {
                var element = this._results.ElementAtOrDefault(i);
                if (element.Value != null)
                {
                    results.Add(element.Key, element.Value);
                    byte[] db;
                    this._results.TryRemove(element.Key, out db);
                }
            }
            if (results.Count != 0)
            {
                result = true;
            }
            return result;
        }

        public void Dispose()
        {
            foreach (var item in this._compressors)
            {
                item.Abort();
            }
        }
    }
}
