using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipStreamMultithread
{
    public class Writer : IDisposable
    {
        private Thread _writer;
        private string _fileName = string.Empty;
        public event EventHandler WriteDone;

        public Writer(string fileName)
        {
            _writer = new Thread(WriteToFile);
            _writer.Name = "Writer";
            _fileName = fileName;
        }

        public void Start(GZipMultithread compressor)
        {
            _writer.Start(compressor);
        }

        private void WriteToFile(object compressorObj)
        {
            try
            {
                GZipMultithread compressor = compressorObj as GZipMultithread;
                using (FileStream archiveFS = File.Create(this._fileName))
                {
                    Console.WriteLine();
                    var currentDataBlock = 0;
                    var percentage = (int)(currentDataBlock / compressor.TaskCount) * 100;
                    Console.Write("\r{0}%", percentage);
                    var tryNumber = 0;
                    long sizeBlockToWrite = 4*1024 * 1024;

                    while (currentDataBlock != compressor.TaskCount && compressor.TaskCount > 0)
                    {
                        if (compressor.CurrentResultCount > 0)
                        {
                            // get data sequence and write
                            MemoryStream ms = new MemoryStream();
                            // try write to memory stream sequence
                            currentDataBlock = compressor.GetResultsToWrite(sizeBlockToWrite, currentDataBlock, out ms);

                            if (ms.Length > 0)
                            {
                                ms.Position = 0;
                                ms.CopyTo(archiveFS);

                                percentage = (int)(((double)currentDataBlock / (double)compressor.TaskCount) * 100);
                                Console.Write("\r{0}%", percentage);
                            }
                            ms.Close();
                        }
                        else
                        {
                            tryNumber++;
                            //Console.WriteLine("Write file wait " + 100 * tryNumber);
                            Thread.Sleep(Math.Min(100 * tryNumber, 1000));
                        }
                    }
                }
                Console.WriteLine();
                EventHandler handler = WriteDone;

                if (handler != null)
                {
                    handler(this, new EventArgs());
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception code: " + ex.HResult + " with message: " + ex.Message);
                throw;
            }
        }

        public void Dispose()
        {
            this._writer.Abort();
        }
    }
}
