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
        static void Main(string[] args)
        {
            Thread.CurrentThread.Name = "Main";
            Thread.CurrentThread.Priority = ThreadPriority.Lowest;
            //leaveOpen in GZipStream is true for MemoryStream output and false for File output
            Console.CancelKeyPress += new ConsoleCancelEventHandler(ConsoleCancelHandler);
            args[0] = "decompress";
            args[1] = "dd.txt.gz";
            //args[1] = "dd3.zip";
            //args[1] = "test6.tar";
            //args[1] = "sample-4294967296";
            //args[1] = "sample-37580963840";
            args[2] = args[1];

            DateTime startTime = DateTime.Now;
            if (args.Length > 0)
            {
                switch (args[0])
                {
                    case "compress":
                        {
                            Console.WriteLine("Start compression...");
                            Console.WriteLine("at time: " + startTime);
                            Compress(args);
                            Console.WriteLine("Compression finished ");
                        }
                        break;
                    case "decompress":
                        {
                            Console.WriteLine("Start decompression...");
                            Console.WriteLine("at time: " + startTime);
                            Decompress(args);
                            Console.WriteLine("Decompression finished ");
                        }
                        break;
                    default:
                        Console.WriteLine("No such parameter");
                        break;
                }
            }
            else
            {
                Console.WriteLine("Not enough parameters");
            }
            DateTime finishTime = DateTime.Now;
            Console.WriteLine("at time: " + finishTime);
            Console.WriteLine("Elapsed time: " + (finishTime - startTime).Duration());
            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }

        private static void Compress(string[] args)
        {
            try
            {
                var fileName = args[1];
                var archiveName = args[2];
                using (FileStream fileFS = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {
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
                            tryNumber++;
                            var sleepTime = Math.Min(100 * tryNumber, maxSleepTime);
                            Thread.Sleep(sleepTime);
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
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception code: " + ex.HResult + " with message: " + ex.Message);
            }
        }

        private static void Decompress(string[] args)
        {
            try
            {
                var fileName = args[1];
                var archiveName = args[2];
                using (FileStream fileFS = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {
                    var compressor = new GZipMultithread(CompressionMode.Decompress);

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
                            tryNumber++;
                            var sleepTime = Math.Min(100 * tryNumber, maxSleepTime);
                            Thread.Sleep(sleepTime);
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
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception code: " + ex.HResult + " with message: " + ex.Message);
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
            Environment.Exit(0);
        }
    }
}
