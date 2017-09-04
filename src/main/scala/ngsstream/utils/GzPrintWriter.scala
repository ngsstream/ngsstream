package ngsstream.utils

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.zip.GZIPOutputStream

class GzPrintWriter(file: File) extends PrintWriter(new GZIPOutputStream(new FileOutputStream(file)))
