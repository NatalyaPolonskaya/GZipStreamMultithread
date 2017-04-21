using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GZipStreamMultithread
{
    class Program
    {
        static int Main(string[] args)
        {
            var result = 0;
            Thread.CurrentThread.Name = "Main";
            Thread.CurrentThread.Priority = ThreadPriority.Lowest;

            Console.CancelKeyPress += new ConsoleCancelEventHandler(ConsoleCancelHandler);

            DateTime startTime = DateTime.Now;
            if (args.Length > 0)
            {
                switch (args[0])
                {
                    case "compress":
                        {
                            Console.WriteLine("Start compression...");
                            Console.WriteLine("at time: " + startTime);
                            result = Compress(args);
                            Console.WriteLine("Compression finished ");
                        }
                        break;
                    case "decompress":
                        {
                            Console.WriteLine("Start decompression...");
                            Console.WriteLine("at time: " + startTime);
                            result = Decompress(args);
                            Console.WriteLine("Decompression finished ");
                        }
                        break;
                    default:
                        {
                            result = 1;
                            Console.WriteLine("No such parameter");
                        }
                        break;
                }
            }
            else
            {
                result = 1;
                Console.WriteLine("Not enough parameters");
            }
            DateTime finishTime = DateTime.Now;
            Console.WriteLine("at time: " + finishTime);
            Console.WriteLine("Elapsed time: " + (finishTime - startTime).Duration());
            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
            return result;
        }

        private static int Compress(string[] args)
        {
            try
            {
                var fileName = args[1];
                var archiveName = args[2];

                var extesion = string.Empty;
                var index = archiveName.LastIndexOf('.');
                if (index >= 0)
                {
                    extesion = archiveName.Substring(index);
                }
                if (extesion != ".gz")
                {
                    archiveName += ".gz";
                }
                using (FileStream fileFS = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {
                    if (fileFS.Length <= 0)
                    {
                        Console.WriteLine("File does not contain data");
                        return 1;
                    }
                    var compressor = new GZipMultithread(CompressionMode.Compress);

                    var writer = new Writer(archiveName);
                    writer.Start(compressor);

                    var maxBufferSize = 128 * 1024;// the best size //1024*1024;// 4 * 1024 * 1024;
                    var buffer = new byte[Math.Min(fileFS.Length, maxBufferSize)];

                    var hasTail = (fileFS.Length % maxBufferSize) > 0 ? 1 : 0;
                    compressor.TaskCount = (fileFS.Length / maxBufferSize) + hasTail;

                    int currentBlockIndex = 0;
                    long start = 0;

                    var tryNumber = 0;
                    var maxSleepTime = 1000;

                    int count = fileFS.Read(buffer, 0, buffer.Length);

                    while (count > 0)
                    {
                        // if current count of tasks over then max tasks number need to stop read
                        if (compressor.CurrentTaskCount > compressor.MaxTaskNumber)
                        {
                            //tryNumber++;
                            //var sleepTime = Math.Min(100 * tryNumber, maxSleepTime);
                            //Thread.Sleep(sleepTime);
                            compressor.StopRead.WaitOne();
                        }
                        else
                        {
                            compressor.AddTask(new DataBlock(currentBlockIndex, buffer));
                            start += count;
                            fileFS.Seek(start, SeekOrigin.Begin);
                            buffer = new byte[Math.Min(fileFS.Length - start, maxBufferSize)];
                            count = fileFS.Read(buffer, 0, buffer.Length);
                            currentBlockIndex++;
                            tryNumber = 0;
                        }
                    }

                    writer.WriteDone += WriteDone;
                    while (!_isWriteDone)
                    {
                        Thread.Sleep(1000);
                    }
                    writer.Dispose();
                    compressor.Dispose();
                }
                GC.Collect();
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception code: " + ex.HResult + " with message: " + ex.Message);
                return 1;
            }
        }

        private static int FindHeaderBegin(byte[] db, int start)
        {
            if (db.Length > 2 && start >= 0)
            {
                for (int i = start; i < db.Length - 2; i++)
                {
                    if (db[i] == 31)
                    {
                        if (db[i + 1] == 139)
                        {
                            if (db[i + 2] == 8)
                            {
                                return i;
                            }
                        }
                    }
                }
            }
            return -1;
        }

        private static int Decompress(string[] args)
        {
            try
            {
                var archiveName = args[1];
                var fileName = args[2];
                using (FileStream fileFS = new FileStream(archiveName, FileMode.Open, FileAccess.Read))
                {
                    if (fileFS.Length <= 0)
                    {
                        Console.WriteLine("File does not contain data");
                        return 1;
                    }
                    var compressor = new GZipMultithread(CompressionMode.Decompress);

                    var writer = new Writer(fileName);
                    writer.Start(compressor);

                    var maxBufferSize = 128 * 1024;// the best size //1024*1024;// 4 * 1024 * 1024;
                    var buffer = new byte[Math.Min(fileFS.Length, maxBufferSize)];

                    var hasTail = (fileFS.Length % maxBufferSize) > 0 ? 1 : 0;



                    var tryNumber = 0;
                    var maxSleepTime = 1000;

                    int currentBlockIndex = 0;
                    long start = 0;

                    int count = fileFS.Read(buffer, 0, buffer.Length);

                    while (count > 0)
                    {
                        // if current count of tasks over then max tasks number need to stop read
                        if (compressor.CurrentTaskCount > compressor.MaxTaskNumber)
                        {
                            tryNumber++;
                            var sleepTime = Math.Min(100 * tryNumber, maxSleepTime);
                            Thread.Sleep(sleepTime);
                        }
                        else
                        {
                            using (MemoryStream data = new MemoryStream())
                            {
                                var startIndex = FindHeaderBegin(buffer, 0);
                                if (startIndex >= 0)
                                {
                                    var endIndex = FindHeaderBegin(buffer, startIndex + 2);
                                    if (endIndex > startIndex)
                                    {
                                        data.Write(buffer, startIndex, endIndex - startIndex);
                                        start += endIndex;
                                    }
                                    else
                                    {
                                        data.Write(buffer, startIndex, count - 1 - startIndex);
                                        start += count;
                                    }
                                }
                                fileFS.Seek(start, SeekOrigin.Begin);
                                count = fileFS.Read(buffer, 0, buffer.Length);
                                compressor.AddTask(new DataBlock(currentBlockIndex, data.ToArray()));
                                compressor.TaskCount++;

                            }
                            currentBlockIndex++;
                            tryNumber = 0;
                        }
                    }

                    writer.WriteDone += WriteDone;
                    while (!_isWriteDone)
                    {
                        Thread.Sleep(1000);
                    }
                    writer.Dispose();
                    compressor.Dispose();
                }
                GC.Collect();
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception code: " + ex.HResult + " with message: " + ex.Message);
                return 1;
            }
        }

        private static bool _isWriteDone { get; set; }

        private static void WriteDone(object sender, EventArgs e)
        {
            _isWriteDone = true;
        }

        protected static void ConsoleCancelHandler(object sender, ConsoleCancelEventArgs args)
        {
            Console.WriteLine("CTRL+C pressed. Process aborted.");
            Environment.Exit(1);
        }
    }
}
