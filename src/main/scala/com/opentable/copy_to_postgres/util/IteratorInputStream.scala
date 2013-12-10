package com.opentable.copy_to_postgres.util

import scala.collection.mutable.ArrayBuffer
import java.io.InputStream

// XXX Should move this to some common utils package
// Crazyness for converting a Scala Stream[String] into a Java InputStream.
class IteratorInputStream(sourceIterator: Iterator[String]) extends InputStream {

  private val NoBufferLines = 100
  private val NewLine = System.getProperty("line.separator")
  
  private var buffer = Array.ofDim[Byte](1024 * 16)
  private var bufferEnd = 0
  private var bufferPos = 0

  override def read: Int = {
    if (bufferEmpty) {
      println("empty")
      val bytesRead = readNextChunk
      if (bytesRead <= 0)
        return -1
    }

    val result = buffer(bufferPos)
    bufferPos += 1 
    result
  }

  private def bufferLength = bufferEnd - bufferPos
  private def bufferEmpty = bufferLength <= 0 
  
  override def read(outBytes: Array[Byte], offset: Int, length: Int): Int = {
    require(offset >= 0, "argument off of the SourceInputStream.read should be >= 0")
    require(length > 0, "argument len of the SourceInputStream.read should be > 0")

    val bytesNeeded = math.min(length, outBytes.size - offset)

    var totalBytesGot = 0
    var bytesGot = 0

    do {
      if (bufferEmpty) readNextChunk
     
      bytesGot = math.min(length - totalBytesGot, bufferLength)

      System.arraycopy(buffer, bufferPos, outBytes, offset + totalBytesGot, bytesGot)
      bufferPos += bytesGot
      totalBytesGot += bytesGot 
    } while (totalBytesGot < bytesNeeded && bytesGot > 0) 
    
    if (totalBytesGot == 0) -1 else totalBytesGot
  }

  private def resizeBuffer(len: Int) = {
    val newBuffer = Array.ofDim[Byte](len * 2)
    buffer.copyToArray(newBuffer)
    buffer = newBuffer
  }

  private def readNextChunk: Int = {
    assert(bufferEmpty)

    if (sourceIterator.hasNext) {
      val bytes:Array[Byte] = getBytes(takeFromSource(NoBufferLines), sourceIterator.hasNext)
      
      if (bytes.isEmpty)
        bufferPos = bufferEnd
      else {
        if (buffer.size < bytes.length*2) 
          resizeBuffer(bytes.length * 2)
        System.arraycopy(bytes, 0, buffer, 0, bytes.length)
        bufferEnd = bytes.length 
        bufferPos = 0
      }
    } else {
      bufferPos = bufferEnd
    }

    // return number of bytes read
    bufferEnd - bufferPos
  }

  private def getBytes(lines: Seq[String], hasNext:Boolean) = {
    (lines.mkString(NewLine) + { if (hasNext) NewLine else "" }).getBytes
  }

  // Since not safe to continue using Iterator after calling it.take we use our own.
  private def takeFromSource(num: Int) = {
    var out = ArrayBuffer[String]()
    var i = num

    while(sourceIterator.hasNext && i > 0) { 
      out += sourceIterator.next
      i -= 1
    }
    out
  }

  override def toString = { s"bp:$bufferPos,be:$bufferEnd,bf:${buffer.take(10).mkString(",")},..." }

}


